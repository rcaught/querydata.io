from datasette import hookimpl
from datasette.utils.asgi import Response


@hookimpl
def register_routes():
    return (
        (
            r"^(?:/aws)(?!_)(?P<rest>.*)$",
            lambda request: Response.redirect(
                "/aws_whats_new{rest}".format(**request.url_vars)
                + (("?" + request.query_string) if request.query_string else ""),
                status=301,
            ),
        ),
    )
