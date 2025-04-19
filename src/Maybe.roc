module [
    Maybe,
    map,
]

Maybe a : [
    Just a,
    Nothing,
]

map : Maybe a, (a -> b) -> Maybe b
map = |ma, f|
    when ma is
        Just(a) -> Just(f(a))
        Nothing -> Nothing
