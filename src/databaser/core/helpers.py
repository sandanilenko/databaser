import asyncio
import logging
import operator
import sys
import uuid
from collections import (
    defaultdict,
    namedtuple,
)
from datetime import (
    datetime,
)
from itertools import (
    chain,
    islice,
)
from typing import (
    Any,
    Iterable,
    Tuple,
    Union, Callable, TypeVar, Coroutine, AsyncIterable,
)

from databaser.settings import ASYNC_SEPARATION_COEFFICIENT, LOG_LEVEL, LOG_DIRECTORY, LOG_FILENAME, TEST_MODE


def make_str_from_iterable(
    iterable: Iterable[Any],
    with_quotes: bool = False,
    quote: str = '"',
) -> str:
    """
    Вспомогательная функция для преобразования итерируемого объекта к строке

    Args:
        iterable: итерируемый объект
        with_quotes: необходимость оборачивания элементов в кавычки
        quote: вид кавычки

    Returns:
        Сформированная строка
    """
    iterable_str = ''

    if iterable:
        if with_quotes:
            iterable_strs = map(lambda item: f'{quote}{item}{quote}', iterable)
        else:
            iterable_strs = map(str, iterable)

        iterable_str = ', '.join(iterable_strs)

    return iterable_str


def dates_to_string(dates_list: Iterable[datetime], format_: str = '%Y-%m-%d %H:%M:%S'):
    """
    Преобразование дат, содержащихся в итерируемом объекте
    """
    return ', '.join(
        map(
            lambda date_: f'{date_:{format_}}',
            dates_list
        )
    )


def topological_sort(
    dependency_pairs: Iterable[Union[str, Tuple[str, str]]],
):
    """
    Сортировка по степени зависимости

    print( topological_sort('aa'.split()) )
    print( topological_sort('ah bg cf ch di ed fb fg hd he ib'.split()) )

    Спасибо Raymond Hettinger
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
    iterable: Iterable[Any],
    size: int,
    is_list: bool = False,
):
    """
    Разделение итерируемого объекта на части указанного в параметрах размера

    Args:
        iterable: итерируемый объект
        size: количество объектов в части
        is_list: преобразовать к спискам формируемые части
    """
    iterator = iter(iterable)

    for first in iterator:
        yield (
            list(chain([first], islice(iterator, size - 1))) if
            is_list else
            chain([first], islice(iterator, size - 1))
        )


T = TypeVar('T')


async def execute_async_function_for_collection(fun: Callable[[T], Coroutine], args: Iterable[T]):
    """
    Позволяет запустить асинхронную функцию одновременно для нескольких элементов.
    Количество одновременных выполнений задаётся через настройки

    Args:
        fun: Функция, которую необходимо применить к элементам объекта
        args: Итерируемый объект
    """

    if ASYNC_SEPARATION_COEFFICIENT <= 0:
        chunks = [args]
    else:
        chunks = make_chunks(args, ASYNC_SEPARATION_COEFFICIENT)

    for chunk in chunks:
        coroutines = [
            asyncio.create_task(fun(i))
            for i in chunk
        ]

        if coroutines:
            await asyncio.wait(coroutines)


async def execute_async_function_for_async_collection(fun: Callable[[T], Coroutine], args: AsyncIterable[T]):
    """
    Позволяет запустить асинхронную функцию одновременно для нескольких элементов.
    Количество одновременных выполнений задаётся через настройки
    В отличие от execute_async_function_for_collection элементы могут быть в асинхронной коллекции

    Args:
        fun: Функция, которую необходимо применить к элементам объекта
        args: Асинхронноитерируемый объект
    """

    coroutines = []
    async for i in args:
        coroutines.append(asyncio.create_task(fun(i)))
        if 0 < ASYNC_SEPARATION_COEFFICIENT <= len(coroutines):
            await asyncio.wait(coroutines)
            coroutines = []

    if coroutines:
        await asyncio.wait(coroutines)


def deep_getattr(object_, attribute_: str, default=None):
    """
    Получить значение атрибута с любого уровня цепочки вложенных объектов.

    Args:
        object_: объект, у которого ищется значение атрибута
        attribute_: атрибут, значение которого необходимо получить (указывается полная цепочка, т.е. 'attr1.attr2.atr3')
        default: значение по умолчанию

    Returns:
        Значение указанного атрибута или значение по умолчанию, если
        атрибут не был найден
    """
    try:
        value = operator.attrgetter(attribute_)(object_)
    except AttributeError:
        value = default

    return value


def add_file_handler_logger(
    directory_path: str,
    file_name: str,
) -> None:
    """
    Добавление логирования в файл на диске

    Args:
        directory_path: полный путь директории с логами
        file_name: имя файла

    """
    if directory_path:
        file_name = f'{file_name}_{uuid.uuid4().hex[:8]}'
        fh = logging.FileHandler(f"{directory_path}/{file_name}.log")
        fh.setFormatter(formatter)
        logger.addHandler(fh)


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
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
add_file_handler_logger(LOG_DIRECTORY, LOG_FILENAME)

if TEST_MODE:
    logger.warning('TEST MODE ACTIVATED!!!')

# Именованный кортеж содержащий результат работы функции топологической сортировки
Results = namedtuple('Results', ['sorted', 'cyclic'])
