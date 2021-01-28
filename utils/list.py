
def chunks(li: list, n: int):
    """Yield successive n-sized chunks from li."""
    for i in range(0, len(li), n):
        yield li[i:i + n]
