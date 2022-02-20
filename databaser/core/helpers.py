import logging
import operator
import os
import sys
from collections import (
    defaultdict,
    namedtuple,
)
from distutils.util import (
    strtobool,
)
from itertools import (
    chain,
    islice,
)
from typing import (
    Iterable,
    List,
    Tuple,
    Union,
)

logger = logging.getLogger('asyncio')

sh = logging.StreamHandler(
    stream=sys.stdout,
)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

sh.setFormatter(formatter)
logger.addHandler(sh)

DBConnectionParameters = namedtuple(
    typename='DBConnectionParameters',
    field_names=[
        'host',
        'port',
        'schema',
        'dbname',
        'user',
        'password',
    ],
)


def make_str_from_iterable(
    iterable: Iterable,
    with_quotes: bool = False,
    quote: str = '"',
):
    iterable_str = ''

    if iterable:
        if with_quotes:
            iterable_strs = map(lambda item: f'{quote}{item}{quote}', iterable)
        else:
            iterable_strs = map(str, iterable)

        iterable_str = ', '.join(iterable_strs)

    return iterable_str


def dates_list_to_str(dates_list, format_='%Y-%m-%d %H:%M:%S'):
    """
    Converting dates list to string by datetime format
    """
    return ', '.join(
        map(
            lambda date_: f'{date_:{format_}}',
            dates_list
        )
    )


Results = namedtuple('Results', ['sorted', 'cyclic'])


def topological_sort(
    dependency_pairs: Iterable[Union[str, Tuple[str, str]]],
):
    """
    Sort values subject to dependency constraints

    print( topological_sort('aa'.split()) )
    print( topological_sort('ah bg cf ch di ed fb fg hd he ib'.split()) )

    Thanks for Raymond Hettinger
    """
    num_heads = defaultdict(int)  # num arrows pointing in
    tails = defaultdict(list)  # list of arrows going out
    heads = []  # unique list of heads in order first seen
    for h, t in dependency_pairs:
        num_heads[t] += 1
        if h in tails:
            tails[h].append(t)
        else:
            tails[h] = [t]
            heads.append(h)

    ordered = [h for h in heads if h not in num_heads]
    for h in ordered:
        for t in tails[h]:
            num_heads[t] -= 1
            if not num_heads[t]:
                ordered.append(t)
    cyclic = [n for n, heads in num_heads.items() if heads]

    return Results(ordered, cyclic)


def make_chunks(
    iterable: Iterable,
    size: int,
    is_list: bool = False,
):
    """
    Efficiently split `iterable` into chunks of size `size`.
    """
    iterator = iter(iterable)

    for first in iterator:
        yield (
            list(chain([first], islice(iterator, size - 1))) if
            is_list else
            chain([first], islice(iterator, size - 1))
        )


def deep_getattr(obj, attr, default=None):
    """
    Получить значение атрибута с любого уровня цепочки вложенных объектов.

    :param object obj: объект, у которого ищется значение атрибута
    :param str attr: атрибут, значение которого необходимо получить (
        указывается полная цепочка, т.е. 'attr1.attr2.atr3')
    :param object default: значение по умолчанию
    :return: значение указанного атрибута или значение по умолчанию, если
        атрибут не был найден
    """
    try:
        value = operator.attrgetter(attr)(obj)
    except AttributeError:
        value = default

    return value


def get_str_environ_parameter(
    name: str,
    default: str = '',
) -> str:
    """
    Getting string environment variable
    """
    return os.environ.get(name, default).strip()


def get_int_environ_parameter(
    name: str,
    default: int = 0,
) -> int:
    """
    Getting integer environment variable
    """
    return int(os.environ.get(name, default))


def get_bool_environ_parameter(
    name: str,
    default: bool = False,
) -> bool:
    """
    Getting boolean environment variable
    """
    parameter_value = os.environ.get(name)

    if parameter_value:
        parameter_value = bool(strtobool(parameter_value))
    else:
        parameter_value = default

    return parameter_value


def get_iterable_environ_parameter(
    name: str,
    separator: str = ',',
    type_=str,
) -> Tuple[str]:
    """
    Getting iterable environment variable as tuple
    """
    return tuple(
        map(
            type_,
            filter(
                None,
                os.environ.get(name, '').replace(' ', '').split(separator)  # noqa
            )
        )
    )


def get_extensible_iterable_environ_parameter(
    name: str,
    separator: str = ',',
    type_=str,
) -> List[str]:
    """
    Getting extensible iterable environment variable as list
    """
    return list(
        map(
            type_,
            filter(
                None,
                os.environ.get(name, '').replace(' ', '').split(separator)  # noqa
            )
        )
    )