"""Partial"""

from querydataio.aws import shared as aws_shared
from querydataio.aws import blogs


def run(print_indent=0) -> bool:
    aws_shared.run_partial(blogs, None, print_indent=print_indent)
