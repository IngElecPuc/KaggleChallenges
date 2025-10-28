cpu_cores = multiprocessing.cpu_count()
usable_cores = max(1, cpu_cores - 1)
print(f"CPU cores disponibles: {cpu_cores} | usaré: {usable_cores}")

