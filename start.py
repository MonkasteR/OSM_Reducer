import multiprocessing
import timeit
from typing import Any, List, Dict
import numpy as np
from numpy import ndarray
import concurrent.futures

start_time: float = timeit.default_timer()


def should_remove_block(lines: List[str], values_to_check: List[str], values_to_address: List[str]) -> bool:
    """
    Проверяет, должен ли блок строк быть удален на основе определенных условий.

    Аргументы:
        lines (List[str]): Список строк для проверки.
        values_to_check (List[str]): Список значений для проверки наличия в строках.
        values_to_address (List[str]): Список значений для обработки, если они присутствуют в строках.

    Возвращает:
        bool: True, если блок должен быть удален, False в противном случае.
    """
    check_found = any(value in line for line in lines for value in values_to_check)
    poi_or_polygon_found = any("[POI]" in line or "[POLYGON]" in line for line in lines)
    addr_found = any(addr in line for line in lines for addr in values_to_address)

    return check_found or (poi_or_polygon_found and not addr_found)


def replace_values(lines: List[str], values_to_replace: List[str]) -> List[str]:
    """
    Заменяет определенные значения в списке строк.

    Аргументы:
        lines (List[str]): Список строк для обработки.
        values_to_replace (List[str]): Список значений для замены.

    Возвращает:
        List[str]: Измененный список строк с замененными значениями.
    """
    arr = np.array(lines)
    mask = np.zeros(len(arr), dtype=bool)
    for value in values_to_replace:
        mask = np.logical_or(mask, [value in line for line in arr])
    return list(arr[~mask])


def process_block(block: List[str], values_to_check: List[str], values_to_replace: List[str],
                  values_to_address: List[str]) -> List[str]:
    """
    Обрабатывает блок кода.

    Аргументы:
        block (List[str]): Блок кода для обработки.
        values_to_check (List[str]): Значения, которые нужно проверить в блоке.
        values_to_replace (List[str]): Значения, которые нужно заменить в блоке.
        values_to_address (List[str]): Значения, которые нужно обработать в блоке.

    Возвращает:
        List[str]: Обработанный блок кода.
    """
    if should_remove_block(block, values_to_check, values_to_address):
        return []
    processed_block = replace_values(block, values_to_replace)
    return processed_block


def process_file(
        in_file: str,
        values_to_check: List[str],
        values_to_replace: Dict[str, str],
        values_to_address: List[str]
) -> None:
    """
    Обрабатывает входной файл и записывает выходной файл.

    Аргументы:
        in_file (str): Путь к входному файлу.
        values_to_check (List[str]): Список значений для проверки в каждом блоке.
        values_to_replace (Dict[str, str]): Словарь значений для замены в каждом блоке.
        values_to_address (List[str]): Список значений для обработки в каждом блоке.

    Возвращает:
        None
    """
    out_file = in_file.replace(".mp", "_out.mp")
    with open(in_file, "r") as f_in, open(out_file, "w") as f_out:
        block = False
        content = []
        for line in f_in:
            if line.startswith("; WayID") or line.startswith("; NodeID"):
                block = True
                temp_block = line
            elif block and line.strip() == "[END]":
                block = False
                if should_remove_block(content, values_to_check, values_to_address):
                    content = []
                else:
                    processed_block = process_block(content, values_to_check, values_to_replace, values_to_address)
                    if processed_block:
                        f_out.write(f"{temp_block}{''.join(processed_block)}{line}")
                    content = []
            elif block:
                content.append(line)
            else:
                f_out.write(line)


if __name__ == "__main__":
    from multiprocessing import freeze_support

    freeze_support()

    with open(file="in_files.txt", mode="r") as f:
        in_files = [line.strip() for line in f.readlines()]
        print("Читаем список обрабатываемых файлов \n")

    with open(file="values_to_check.txt", mode="r") as f:
        values_to_check = [line.strip() for line in f.readlines()]
        print("Читаем список слов удаляющих блок \n")

    with open(file="values_to_replace.txt", mode="r") as f:
        values_to_replace = [line.strip() for line in f.readlines()]
        print("Читаем список слов для замены \n")

    with open(file="values_to_address.txt", mode="r") as f:
        values_to_address: list[str] = [line.strip() for line in f.readlines()]
        print("Читаем список слов удаляющих полигон \n")

    with multiprocessing.Pool() as pool:
        pool.starmap(
            process_file, [(in_file, values_to_check, values_to_replace, values_to_address) for in_file in in_files]
        )


    # with concurrent.futures.ProcessPoolExecutor() as executor:
    #     futures = [executor.submit(process_file, in_file, values_to_check, values_to_replace, values_to_address) for
    #                in_file in in_files]
    #     results = [future.result() for future in concurrent.futures.as_completed(futures)]

    end_time: float = timeit.default_timer()
    print("Время выполнения программы: {:.5f} секунд".format(end_time - start_time))
