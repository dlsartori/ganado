import inspect
import krnl_db_query
import krnl_config
import krnl_custom_types

def is_relevant(obj):
    """Filter for the inspector to filter out non user defined functions/classes"""
    if hasattr(obj, '__name__') and obj.__name__ == 'type':
        return False
    if inspect.isfunction(obj) or inspect.isclass(obj) or inspect.ismethod(obj):
        return True


def print_docs(module):
    default = 'No doc string provided'  # Default if there is no docstring, can be removed if you want
    flag = True

    for child in inspect.getmembers(module, is_relevant):
        if not flag:
            print('\n\n\n')
        flag = False  # To avoid the newlines at top of output
        doc = inspect.getdoc(child[1])
        if not doc:
            doc = default
        print(child[0], doc, sep = '\n')

        if inspect.isclass(child[1]):
            for grandchild in inspect.getmembers(child[1], is_relevant):
                doc = inspect.getdoc(grandchild[1])
                if doc:
                    doc = doc.replace('\n', '\n    ')
                else:
                    doc = default
                print('\n    ' + grandchild[0], doc, sep='\n    ')


if __name__ == '__main__':
    # 1. Import class(es) for which docstrings are required.

    from krnl_tag import Tag
    from krnl_tag_activity import TagActivity
    from krnl_abstract_class_animal import Animal
    from krnl_abstract_class_activity import Activity
    from krnl_animal_activity import AnimalActivity
    from krnl_bovine import Bovine
    from krnl_bovine_activity import BovineActivity
    class_list = [Tag, TagActivity, Activity, Animal, AnimalActivity, BovineActivity, Bovine]
    # 2. print docstrings for class.
    print('\n')
    for j in class_list:
        print(f'\n*********************************** Class: {j.__qualname__} *******************************************')
        print_docs(j)
        print(f'\n                                 End class {j.__qualname__}')
        print('-------------------------------------------------------------------------------------------------------------\n')
