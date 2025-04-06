app [Model, init!, respond!] {
    pf: platform "https://github.com/roc-lang/basic-webserver/releases/download/0.12.0/Q4h_In-sz1BqAvlpmCsBHhEJnn_YvfRRMiNACB_fBbk.tar.br",
    pg: "../src/main.roc",
}

import pf.Stdout
import pg.Pg.Client exposing [Client]
import pg.Pg.Cmd
import pg.Pg.Result

Model : {
    pg_client : Client,
}

init! = |_|
    _ = Stdout.line!("Start!")

    # NOTE: The Tcp.Stream is closed as soon as the last reference to it is dropped.
    # We therefore connect in `init!` and store the connection in our `Model` so that
    # we can keep it going until the end of the program.
    #
    # For some reason `basic-webserver` dies if the Tcp.Stream is closed. I don't know
    # why. This prevents us from connecting to pg during a request for example, as the
    # connection's `Tcp.Stream` would be dropped at the end of the request and
    # `basic-webserver` would die.
    #
    # There is no error message or anything when this issue occurs.
    # NOTE: `connect!` can make the process exit without an error.
    client = Pg.Client.connect!(
        {
            host: "localhost",
            port: 5432,
            user: "postgres",
            auth: None,
            database: "postgres",
        },
    )

    _ =
        when client is
            Ok(_) ->
                Stdout.line!("Success!")

            Err(_) ->
                Stdout.line!("Failure!")

    _ = Stdout.line!("Connected!")

    Ok({ pg_client: client? })

respond! = |_request, model|
    rows =
        Pg.Cmd.new(
            """
            select $1 as name, $2 as age
            union all
            select 'Julio' as name, 23 as age
            """,
        )
        |> Pg.Cmd.bind([Pg.Cmd.str("John"), Pg.Cmd.u8(32)])
        |> Pg.Cmd.expect_n(
            (
                Pg.Result.succeed(
                    |name|
                        |age|
                            age_str = Num.to_str(age)

                            "${name}: ${age_str}",
                )
                |> Pg.Result.with(Pg.Result.str("name"))
                |> Pg.Result.with(Pg.Result.u8("age"))
            ),
        )
        |> Pg.Client.command!(model.pg_client)?

    s = Str.join_with(rows, "\n")
    _ = Stdout.line!(s)

    Ok(
        {
            status: 200,
            headers: [],
            body: Str.to_utf8(s),
        },
    )
