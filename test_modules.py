from krnl_initializer import module_loader
from krnl_tag import Tag
# import inspect


if __name__ == '__main__':
    print(f'locals before update(): {locals()}')
    print(f'In Tags. Initial class_register: {Tag.getTagClasses()}\n')
    attr_dict = module_loader()
    locals().update(attr_dict)          # Updates the locals dict for the module.
    print(f'locals after update(): {locals()}')

    # Database records will store an object-definition string such as 'TagsRFID' that will match the key in attr_dict,
    # enabling the creation of objects via a factory using the data in locals(), vars() or attr_dict. See what's best.
    # obj = vars()['TagsRFID']()      # Creates object of class TagsRFID.
    if attr_dict:
        obj = attr_dict['TagsRFID']()  # Creates object of class TagsRFID.
        print(f'\nobj type: {type(obj)}')
        obj.method1()                   # obj calls class method.
        input('Now we go loading again. Press any key to continue...')
        attr_dict = module_loader()
    else:
        print(f'No modules loaded this time. ')

    print(f'In Tags, class_register after module loading: {Tag.getTagClasses()}\nBye, bye...')
