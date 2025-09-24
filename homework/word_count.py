# """Taller evaluable"""

# pylint: disable=broad-exception-raised

import glob
import os
import string
import time


def copy_raw_files_to_input_folder(n=1000):
    """Copia n veces los archivos de files/raw a files/input"""
    if os.path.exists("files/input/"):
        for file in glob.glob("files/input/*"):
            os.remove(file)
    else:
        os.makedirs("files/input")

    for file in glob.glob("files/raw/*"):
        with open(file, "r", encoding="utf-8") as f:
            text = f.read()

        for i in range(1, n + 1):
            raw_filename_with_extension = os.path.basename(file)
            raw_filename_without_extension = os.path.splitext(
                raw_filename_with_extension
            )[0]
            new_filename = f"{raw_filename_without_extension}_{i}.txt"
            with open(f"files/input/{new_filename}", "w", encoding="utf-8") as f2:
                f2.write(text)


def run_job(input_path: str, output_path: str):
    """Ejecuta el proceso completo de MapReduce"""
    start_time = time.time()

    # Lee los archivos de input_path
    sequence = []
    files = glob.glob(f"{input_path}/*")
    for file in files:
        with open(file, "r", encoding="utf-8") as f:
            for line in f:
                sequence.append((file, line))

    # Mapper: convierte líneas en pares (palabra, 1)
    pairs_sequence = []
    for _, line in sequence:
        line = line.lower()
        line = line.translate(str.maketrans("", "", string.punctuation))
        line = line.replace("\n", "")
        words = line.split()
        pairs_sequence.extend((word, 1) for word in words)

    # Shuffle & Sort
    pairs_sequence = sorted(pairs_sequence)

    # Reducer: acumula conteo de palabras
    result = []
    for key, value in pairs_sequence:
        if result and result[-1][0] == key:
            result[-1] = (key, result[-1][1] + value)
        else:
            result.append((key, value))

    # Prepara la carpeta de salida
    if os.path.exists(output_path):
        for file in glob.glob(f"{output_path}/*"):
            os.remove(file)
    else:
        os.makedirs(output_path)

    # Guarda resultados en part-00000
    with open(f"{output_path}/part-00000", "w", encoding="utf-8") as f:
        for key, value in result:
            f.write(f"{key}\t{value}\n")

    # Marca éxito
    with open(f"{output_path}/_SUCCESS", "w", encoding="utf-8") as f:
        f.write("")

    end_time = time.time()
    print(f"Tiempo de ejecución: {end_time - start_time:.2f} segundos")
