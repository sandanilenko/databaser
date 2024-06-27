import os
from distutils.util import strtobool
from typing import List, Tuple


def get_str_environ_parameter(
    name: str,
    default: str = '',
) -> str:
    """
    Получение значения параметра из переменных окружения, имеющего строковое значение

    Args:
        name: имя переменной окружения
        default: значение по-умолчанию

    Returns:
        Полученное значение
    """

    return os.environ.get(name, default).strip()


def get_int_environ_parameter(
    name: str,
    default: int = 0,
) -> int:
    """
    Получение значения параметра из переменных окружения, имеющего целочисленное значение

    Args:
        name: имя переменной окружения
        default: значение по-умолчанию

    Returns:
        Полученное значение
    """

    return int(os.environ.get(name, default))


def get_bool_environ_parameter(
    name: str,
    default: bool = False,
) -> bool:
    """
    Получение значения параметра из переменных окружения, имеющего булево значение

    Args:
        name: имя переменной окружения
        default: значение по-умолчанию

    Returns:
        Полученное значение
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
) -> Tuple[str, ...]:
    """
    Получение значения параметра из переменных окружения, имеющего строковое значение, преобразованное к кортежу

    Args:
        name: имя переменной окружения
        separator: разделитель
        type_: результирующий тип элементов
    Returns:
        Полученное значение
    """

    return tuple(
        map(
            type_,
            filter(
                None,
                os.environ.get(name, '').replace(' ', '').split(separator)
            )
        )
    )


def get_extensible_iterable_environ_parameter(
    name: str,
    separator: str = ',',
    type_=str,
) -> List[str]:
    """
    Получение значения параметра из переменных окружения, имеющего строковое значение, преобразованное к списку

    Args:
        name: имя переменной окружения
        separator: разделитель
        type_: результирующий тип элементов

    Returns:
        Полученное значение
    """

    return list(
        map(
            type_,
            filter(
                None,
                os.environ.get(name, '').replace(' ', '').split(separator)
            )
        )
    )

