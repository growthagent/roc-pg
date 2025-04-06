## Unfortunately, the regular `Pg.Client` module  runs into the
## infamous "Error during alias analysis" compiler bug when used from basic-cli.
## This version does not.
module [
    connect!,
    command!,
    batch!,
    prepare!,
    Error,
    error_to_str,
    Client,
]

import Protocol.Backend
import Protocol.Frontend
import Bytes.Encode
import Bytes.Decode exposing [decode]
import Pg.Result exposing [CmdResult]
import Pg.Cmd exposing [Cmd]
import Pg.Batch exposing [Batch]
import pf.Tcp
import Cmd
import Batch

Client := {
    stream : Tcp.Stream,
    backend_key : Result Protocol.Backend.KeyData [Pending],
}

connect! :
    {
        host : Str,
        port : U16,
        user : Str,
        auth ?? [None, Password Str],
        database : Str,
    }
    => Result Client _
connect! = |{ host, port, database, auth ?? None, user }|
    stream = Tcp.connect!(host, port)?

    Tcp.write!(stream, Protocol.Frontend.startup({ user, database }))?

    message_loop!(
        stream,
        {
            parameters: Dict.empty({}),
            backend_key: Err(Pending),
        },
        |msg, state|
            when msg is
                AuthOk ->
                    next(state)

                AuthCleartextPassword ->
                    when auth is
                        None ->
                            Err(PasswordRequired)

                        Password(pwd) ->
                            # TODON: I'm throwing here
                            Tcp.write!(stream, Protocol.Frontend.password_message(pwd))?

                            next(state)

                AuthUnsupported ->
                    Err(UnsupportedAuth)

                BackendKeyData(backend_key) ->
                    next({ state & backend_key: Ok(backend_key) })

                ReadyForQuery(_) ->
                    client = @Client(
                        {
                            stream,
                            backend_key: state.backend_key,
                        },
                    )

                    return_(client)

                _ ->
                    unexpected(msg),
    )

# Single command

command! :
    Cmd a err,
    Client
    => Result
        a
        [
            PgExpectErr err,
            PgErr Error,
            PgProtoErr _,
            TcpReadErr _,
            TcpUnexpectedEOF,
            TcpWriteErr _,
        ]
command! = |cmd, @Client({ stream })|
    { kind, limit, bindings } = Cmd.params(cmd)
    { format_codes, param_values } = Cmd.encode_bindings(bindings)

    init =
        when kind is
            SqlCmd(sql) ->
                {
                    messages: Bytes.Encode.sequence(
                        [
                            Protocol.Frontend.parse({ sql }),
                            Protocol.Frontend.bind({ format_codes, param_values }),
                            Protocol.Frontend.describe_portal({}),
                            Protocol.Frontend.execute({ limit }),
                        ],
                    ),
                    fields: [],
                }

            PreparedCmd(prepared) ->
                {
                    messages: Bytes.Encode.sequence(
                        [
                            Protocol.Frontend.bind(
                                {
                                    format_codes,
                                    param_values,
                                    prepared_statement: prepared.name,
                                },
                            ),
                            Protocol.Frontend.execute({ limit }),
                        ],
                    ),
                    fields: prepared.fields,
                }

    # TODON: I'm throwing here
    send_with_sync!(stream, init.messages)?

    # TODON: I'm throwing here
    result = read_cmd_result!(init.fields, stream)?

    decoded =
        Cmd.decode(result, cmd)
        # TODON: I'm throwing here
        |> Result.map_err(PgExpectErr)?
    # TODON: Should be fine to just skip this?
    # |> Task.from_result!

    # TODON: I'm throwing here
    read_ready_for_query!(stream)?

    Ok(decoded)

# Batches

batch! :
    Batch a err,
    Client
    => Result
        a
        [
            PgExpectErr err,
            PgErr Error,
            PgProtoErr _,
            TcpReadErr _,
            TcpUnexpectedEOF,
            TcpWriteErr _,
        ]
batch! = |cmd_batch, @Client({ stream })|
    { commands, seen_sql, decode: batch_decode } = Batch.params(cmd_batch)

    reused_indexes =
        seen_sql
        |> Dict.walk(
            Set.empty({}),
            |set, _, { index, reused }|
                if reused then
                    set |> Set.insert(index)
                else
                    set,
        )

    inits =
        commands
        |> List.map_with_index(|cmd, ix| init_batched_cmd(reused_indexes, cmd, ix))

    command_messages =
        inits
        |> List.map(.messages)
        |> Bytes.Encode.sequence

    close_messages =
        reused_indexes
        |> Set.to_list
        |> List.map(
            |ix|
                Protocol.Frontend.close_statement({ name: Batch.reuse_name(ix) }),
        )
        |> Bytes.Encode.sequence

    messages = command_messages |> List.concat(close_messages)
    # TODON: I'm throwing here
    send_with_sync!(stream, messages)?

    loop!(
        {
            remaining: inits,
            results: List.with_capacity(List.len(commands)),
        },
        |state| batch_read_step!(batch_decode, stream, state),
    )

loop! : state, (state => Result [Step state, Done done] err) => Result done err
loop! = |state, fn!|
    when fn!(state) is
        Err(err) -> Err(err)
        Ok(Done(done)) -> Ok(done)
        Ok(Step(next_)) -> loop!(next_, fn!)

init_batched_cmd :
    Set U64,
    Batch.BatchedCmd,
    U64
    -> {
        messages : List U8,
        fields : [
            Describe,
            ReuseFrom U64,
            Known (List Pg.Result.RowField),
        ],
    }
init_batched_cmd = |reused_indexes, cmd, cmd_index|
    { format_codes, param_values } = Cmd.encode_bindings(cmd.bindings)

    when cmd.kind is
        SqlCmd(sql) ->
            name =
                if Set.contains(reused_indexes, cmd_index) then
                    Batch.reuse_name(cmd_index)
                else
                    ""

            {
                messages: Bytes.Encode.sequence(
                    [
                        Protocol.Frontend.parse({ sql, name }),
                        Protocol.Frontend.bind(
                            {
                                format_codes,
                                param_values,
                                prepared_statement: name,
                            },
                        ),
                        Protocol.Frontend.describe_portal({}),
                        Protocol.Frontend.execute({ limit: cmd.limit }),
                    ],
                ),
                fields: Describe,
            }

        ReuseSql(index) ->
            {
                messages: Bytes.Encode.sequence(
                    [
                        Protocol.Frontend.bind(
                            {
                                format_codes,
                                param_values,
                                prepared_statement: Batch.reuse_name(index),
                            },
                        ),
                        Protocol.Frontend.execute({ limit: cmd.limit }),
                    ],
                ),
                fields: ReuseFrom(index),
            }

        PreparedCmd(prepared) ->
            {
                messages: Bytes.Encode.sequence(
                    [
                        Protocol.Frontend.bind(
                            {
                                format_codes,
                                param_values,
                                prepared_statement: prepared.name,
                            },
                        ),
                        Protocol.Frontend.execute({ limit: cmd.limit }),
                    ],
                ),
                fields: Known(prepared.fields),
            }

batch_read_step! = |batch_decode, stream, { remaining, results }|
    when remaining is
        [] ->
            when batch_decode(results) is
                Ok({ value }) ->
                    # TODON: I'm throwing here
                    read_ready_for_query!(stream)?
                    return_(value)

                Err(MissingCmdResult(index)) ->
                    Err(PgProtoErr(MissingBatchedCmdResult(index)))

                Err(ExpectErr(err)) ->
                    Err(PgExpectErr(err))

        [first, ..] ->
            fields = batched_cmd_fields(results, first.fields)?
            # TODON: I'm throwing here
            result = read_cmd_result!(fields, stream)?

            next(
                {
                    remaining: remaining |> List.drop_first(1),
                    results: results |> List.append(result),
                },
            )

batched_cmd_fields = |results, fields_method|
    when fields_method is
        Describe ->
            Ok([])

        ReuseFrom(index) ->
            when List.get(results, index) is
                Ok(result) ->
                    Ok(Pg.Result.fields(result))

                Err(OutOfBounds) ->
                    # TODO: better name
                    Err(PgProtoErr(ResultOutOfBounds))

        Known(fields) ->
            Ok(fields)

# Execute helpers

read_cmd_result! = |init_fields, stream|
    message_loop!(
        stream,
        {
            fields: init_fields,
            rows: [],
        },
        |msg, state|
            when msg is
                ParseComplete | BindComplete | ParameterDescription | NoData ->
                    next(state)

                RowDescription(fields) ->
                    next({ state & fields: fields })

                DataRow(row) ->
                    next({ state & rows: List.append(state.rows, row) })

                CommandComplete(_) | EmptyQueryResponse | PortalSuspended ->
                    return_(Pg.Result.create(state))

                _ ->
                    unexpected(msg),
    )

read_ready_for_query! = |stream|
    message_loop!(
        stream,
        {},
        |msg, {}|
            when msg is
                CloseComplete ->
                    next({})

                ReadyForQuery(_) ->
                    return_({})

                _ ->
                    unexpected(msg),
    )

# Prepared Statements

prepare! :
    Str,
    { name : Str, client : Client }
    => Result
        (Cmd CmdResult [])
        [
            PgErr Error,
            PgProtoErr _,
            TcpReadErr _,
            TcpUnexpectedEOF,
            TcpWriteErr _,
        ]
prepare! = |sql, { name, client }|
    @Client({ stream }) = client

    parse_and_describe = Bytes.Encode.sequence(
        [
            Protocol.Frontend.parse({ sql, name }),
            Protocol.Frontend.describe_statement({ name }),
            Protocol.Frontend.sync,
        ],
    )

    # TODON: I'm throwing here
    Tcp.write!(stream, parse_and_describe)?

    message_loop!(
        stream,
        [],
        |msg, state|
            when msg is
                ParseComplete | ParameterDescription | NoData ->
                    next(state)

                RowDescription(fields) ->
                    next(fields)

                ReadyForQuery(_) ->
                    return_(Cmd.prepared({ name, fields: state }))

                _ ->
                    unexpected(msg),
    )

# Errors

Error : Protocol.Backend.Error

error_to_str : Error -> Str
error_to_str = |err|
    add_field = |str, name, result|
        when result is
            Ok(value) ->
                "${str}\n${name}: ${value}"

            Err({}) ->
                str

    fields_str =
        ""
        |> add_field("Detail", err.detail)
        |> add_field("Hint", err.hint)
        |> add_field("Position", (err.position |> Result.map_ok(Num.to_str)))
        |> add_field("Internal Position", (err.internal_position |> Result.map_ok(Num.to_str)))
        |> add_field("Internal Query", err.internal_query)
        |> add_field("Where", err.ewhere)
        |> add_field("Schema", err.schema_name)
        |> add_field("Table", err.table_name)
        |> add_field("Data type", err.data_type_name)
        |> add_field("Constraint", err.constraint_name)
        |> add_field("File", err.file)
        |> add_field("Line", err.line)
        |> add_field("Routine", err.line)

    "${err.localized_severity} (${err.code}): ${err.message}\n${fields_str}"
    |> Str.trim

# Helpers

read_message! : Tcp.Stream => Result Protocol.Backend.Message [PgProtoErr _, TcpReadErr _, TcpUnexpectedEOF]
read_message! = |stream|
    # TODON: I'm throwing here
    header_bytes = Tcp.read_exactly!(stream, 5)?

    proto_decode = |bytes, dec|
        decode(bytes, dec)
        |> Result.map_err(PgProtoErr)
    # TODON: Should be safe to skip this?
    # |> Task.from_result

    # TODON: I'm throwing here
    meta = header_bytes |> proto_decode(Protocol.Backend.header)?

    if meta.len > 0 then
        # TODON: I'm throwing here
        payload = Tcp.read_exactly!(stream, Num.to_u64(meta.len))?
        proto_decode(payload, Protocol.Backend.message(meta.msg_type))
    else
        proto_decode([], Protocol.Backend.message(meta.msg_type))

message_loop! : Tcp.Stream, state, (Protocol.Backend.Message, state => Result [Done done, Step state] _) => Result done _
message_loop! = |stream, init_state, step_fn!|
    loop!(
        init_state,
        |state|
            # TODON: I'm throwing here
            message = read_message!(stream)?

            when message is
                ErrorResponse(error) ->
                    Err(PgErr(error))

                ParameterStatus(_) ->
                    Ok(Step(state))

                _ ->
                    step_fn!(message, state),
    )

next : a -> Result [Step a] *
next = |state|
    Ok(Step(state))

return_ : a -> Result [Done a] *
return_ = |result|
    Ok(Done(result))

# TODON: Doesn't _have_ to be impure
unexpected : a -> Result * [PgProtoErr [UnexpectedMsg a]]
unexpected = |msg|
    Err(PgProtoErr(UnexpectedMsg(msg)))

send_with_sync! : Tcp.Stream, List U8 => Result {} _
send_with_sync! = |stream, bytes|
    content = Bytes.Encode.sequence(
        [
            bytes,
            Protocol.Frontend.sync,
        ],
    )

    Tcp.write!(stream, content)
