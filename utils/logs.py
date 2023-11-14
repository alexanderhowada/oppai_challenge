def print_args(print_args=[], print_kwargs=[], sep='-'*20):
    """Wraps the function and prints log message.
    
    Args:
        args: arguments to print [1, 2, 3, ...].
        sep: separator to print before and after calling the function.

    Returns:
        callable.
    """

    def decorator(f):
        def wrap(*args, **kwargs):
            print(sep)
            for i in print_args:
                print(args[i])
            for key in print_kwargs:
                print(f"{key}: {kwargs[key]}")

            res = f(*args, **kwargs)
            print(sep)
            return res
        return wrap
    return decorator