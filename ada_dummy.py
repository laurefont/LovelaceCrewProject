
def foo(a: int)-> int:
    """
    This function is here to make python parser raise a SyntaxError on the cluster
    """
    return a + 1


b = foo(0)
