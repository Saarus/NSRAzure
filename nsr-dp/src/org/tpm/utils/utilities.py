import collections


def converttostr(data):
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(converttostr, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(converttostr, data))
    else:
        return data