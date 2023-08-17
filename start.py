import multiprocessing
import timeit

import numpy as np
from numpy import ndarray

start_time: float = timeit.default_timer()


def should_remove_block(lines, values_to_check, values_to_address) -> bool:
    check_found: bool = any(value in str(lines) for value in values_to_check)
    poi_or_polygon_found = any("[POI]" in line or "[POLYGON]" in line for line in lines)

    return check_found or (poi_or_polygon_found and not all(addr in str(lines) for addr in values_to_address))


def replace_values(lines, values_to_replace):
    arr: ndarray = np.array(lines)
    mask = np.zeros(len(arr), dtype=bool)
    for value in values_to_replace:
        mask = np.logical_or(mask, [value in line for line in arr])
    return list(arr[~mask])


def process_block(block, values_to_check, values_to_replace, values_to_address):
    if should_remove_block(block, values_to_check, values_to_address):
        return []
    else:
        processed_block = replace_values(block, values_to_replace)
        return processed_block


def process_file(in_file, values_to_check, values_to_replace, values_to_address):
    out_file = in_file.replace(".mp", "_out.mp")
    with open(file=in_file, mode="r") as f_in, open(file=out_file, mode="w") as f_out:
        block = False
        content = []
        for line in f_in:
            if line.startswith("; WayID") or line.startswith("; NodeID"):
                block = True
                temp_block: str = line
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
    f_in.close()
    f_out.close()


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

    end_time: float = timeit.default_timer()
    print("Время выполнения программы: {:.5f} секунд".format(end_time - start_time))
