module [
    CmdResult,
    RowField,
    create,
    len,
    fields,
    rows,
    decode,
    Decode,
    str,
    u8,
    u16,
    u32,
    u64,
    u128,
    i8,
    i16,
    i32,
    i64,
    i128,
    f32,
    f64,
    dec,
    bool,
    with,
    apply,
    succeed,
]

import Protocol.Backend

RowField : Protocol.Backend.RowField

CmdResult := {
    fields : List RowField,
    rows : List (List (List U8)),
}

create = @CmdResult

fields : CmdResult -> List RowField
fields = |@CmdResult(result)| result.fields

rows : CmdResult -> List (List (List U8))
rows = |@CmdResult(result)| result.rows

len : CmdResult -> U64
len = |@CmdResult(result)|
    List.len(result.rows)

Decode a err :=
    List RowField
    ->
    Result
        (List (List U8)
            ->
            Result a [FieldNotFound Str]err)
        [FieldNotFound Str]

decode : CmdResult, Decode a err -> Result (List a) [FieldNotFound Str]err
decode = |@CmdResult(r), @Decode(get_decode)|
    when get_decode(r.fields) is
        Ok(fn) ->
            List.map_try(r.rows, fn)

        Err(FieldNotFound(name)) ->
            Err(FieldNotFound(name))

str = decoder(Ok)

u8 = decoder(Str.to_u8)

u16 = decoder(Str.to_u16)

u32 = decoder(Str.to_u32)

u64 = decoder(Str.to_u64)

u128 = decoder(Str.to_u128)

i8 = decoder(Str.to_i8)

i16 = decoder(Str.to_i8)

i32 = decoder(Str.to_i32)

i64 = decoder(Str.to_i64)

i128 = decoder(Str.to_i128)

f32 = decoder(Str.to_f32)

f64 = decoder(Str.to_f64)

dec = decoder(Str.to_dec)

bool = decoder(
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
                                    Ok(bytes) ->
                                        when Str.from_utf8(bytes) is
                                            Ok(str_value) ->
                                                fn(str_value)

                                            Err(err) ->
                                                Err(err)

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
