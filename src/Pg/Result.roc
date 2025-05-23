module [
    CmdResult,
    Decode,
    ParameterField,
    RowField,
    apply,
    bool,
    create,
    custom,
    dec,
    decode,
    f32,
    f64,
    fields,
    i128,
    i16,
    i32,
    i64,
    i8,
    len,
    map,
    maybe_bool,
    maybe_custom,
    maybe_dec,
    maybe_f32,
    maybe_f64,
    maybe_i128,
    maybe_i16,
    maybe_i32,
    maybe_i64,
    maybe_i8,
    maybe_str,
    maybe_u128,
    maybe_u16,
    maybe_u32,
    maybe_u64,
    maybe_u8,
    record_builder,
    rows,
    str,
    succeed,
    u128,
    u16,
    u32,
    u64,
    u8,
    with,
]

import Protocol.Backend
import Maybe exposing [Maybe]

RowField : Protocol.Backend.RowField
ParameterField : Protocol.Backend.ParameterField

CmdResult := {
    fields : List RowField,
    rows : List (List (Maybe (List U8))),
    parameters : List ParameterField,
}

create = @CmdResult

fields : CmdResult -> List RowField
fields = |@CmdResult(result)| result.fields

rows : CmdResult -> List (List (Maybe (List U8)))
rows = |@CmdResult(result)| result.rows

len : CmdResult -> U64
len = |@CmdResult(result)|
    List.len(result.rows)

Decode a err :=
    List RowField
    ->
    Result
        (List (Maybe (List U8)) -> Result a [FieldNotFound Str]err)
        [FieldNotFound Str]

decode : CmdResult, Decode a err -> Result (List a) [FieldNotFound Str]err
decode = |@CmdResult(r), @Decode(get_decode)|
    when get_decode(r.fields) is
        Ok(fn) ->
            List.map_try(r.rows, fn)

        Err(FieldNotFound(name)) ->
            Err(FieldNotFound(name))

maybe = |fn|
    decoder(
        |m_str|
            when m_str is
                Just(s) -> Ok(Just(fn(s)?))
                Nothing -> Ok(Nothing),
    )

non_maybe = |fn|
    decoder(
        |m_str|
            when m_str is
                Just(s) -> fn(s)
                Nothing -> Err(UnexpectedNull),
    )

str = non_maybe(Ok)
maybe_str = maybe(Ok)

u8 = non_maybe(Str.to_u8)
maybe_u8 = maybe(Str.to_u8)

u16 = non_maybe(Str.to_u16)
maybe_u16 = maybe(Str.to_u16)

u32 = non_maybe(Str.to_u32)
maybe_u32 = maybe(Str.to_u32)

u64 = non_maybe(Str.to_u64)
maybe_u64 = maybe(Str.to_u64)

u128 = non_maybe(Str.to_u128)
maybe_u128 = maybe(Str.to_u128)

i8 = non_maybe(Str.to_i8)
maybe_i8 = maybe(Str.to_i8)

i16 = non_maybe(Str.to_i16)
maybe_i16 = maybe(Str.to_i16)

i32 = non_maybe(Str.to_i32)
maybe_i32 = maybe(Str.to_i32)

i64 = non_maybe(Str.to_i64)
maybe_i64 = maybe(Str.to_i64)

i128 = non_maybe(Str.to_i128)
maybe_i128 = maybe(Str.to_i128)

f32 = non_maybe(Str.to_f32)
maybe_f32 = maybe(Str.to_f32)

f64 = non_maybe(Str.to_f64)
maybe_f64 = maybe(Str.to_f64)

dec = non_maybe(Str.to_dec)
maybe_dec = maybe(Str.to_dec)

custom = |name, fn| non_maybe(fn)(name)
maybe_custom = |name, fn| maybe(fn)(name)

bool = non_maybe(
    |v|
        when v is
            "t" -> Ok(Bool.true)
            "f" -> Ok(Bool.false)
            _ -> Err(InvalidBoolStr),
)
maybe_bool = maybe(
    |v|
        when v is
            "t" -> Ok(Bool.true)
            "f" -> Ok(Bool.false)
            _ -> Err(InvalidBoolStr),
)

decoder = |fn|
    |name|
        @Decode(
            |row_fields|
                when List.find_first_index(row_fields, |f| f.name == name) is
                    Ok(index) ->
                        Ok(
                            |row|
                                when List.get(row, index) is
                                    Ok(m_bytes) ->
                                        m_str =
                                            when m_bytes is
                                                Just(bytes) -> Just(Str.from_utf8(bytes)?)
                                                Nothing -> Nothing

                                        Result.map_err(fn(m_str), |error| DecodeErr({ column: name, error }))

                                    Err(OutOfBounds) ->
                                        Err(FieldNotFound(name)),
                        )

                    Err(NotFound) ->
                        Err(FieldNotFound(name)),
        )

map2 = |@Decode(a), @Decode(b), cb|
    @Decode(
        |row_fields|
            Result.try(
                a(row_fields),
                |decode_a|
                    Result.try(
                        b(row_fields),
                        |decode_b|
                            Ok(
                                |row|
                                    Result.try(
                                        decode_a(row),
                                        |value_a|
                                            Result.try(
                                                decode_b(row),
                                                |value_b|
                                                    Ok(cb(value_a, value_b)),
                                            ),
                                    ),
                            ),
                    ),
            ),
    )

succeed = |value|
    @Decode(|_| Ok(|_| Ok(value)))

with = |a, b| map2(a, b, |fn, val| fn(val))

apply = |a| |fn| with(fn, a)

map = |a, fn| with(succeed(fn), a)

## Use with Roc's [Record Builder](https://www.roc-lang.org/tutorial#record-builder)
## syntax to build records of your returned rows:
##
## ```
## Pg.Cmd.expect_n(
##     { Pg.Result.record_builder <-
##         name: Pg.Result.str("name"),
##         age: Pg.Result.u8("age"),
##     },
## )
## ```
# NOTE: `record_builder` is an alias of `map2` simply to increase its
# discoverability and user-friendliness for its intended use-case.
record_builder = map2
