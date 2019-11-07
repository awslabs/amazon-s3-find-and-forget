def paginate(client, method, iter_keys, **kwargs):
    paginator = client.get_paginator(method.__name__)
    page_iterator = paginator.paginate(**kwargs)
    keys = list(iter_keys)
    for page in page_iterator:
        for key in keys:
            page = page[key]
        for result in page:
            yield result

