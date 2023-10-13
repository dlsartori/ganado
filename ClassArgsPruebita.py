
if __name__ == '__main__':

    class Activity:
        def __init__(self):
            pass

    class Tag:
        def __init__(self, val):
            self.tagID = val

        def inventory(self):     #  def get_inner_object(obj): Aqui invoca el Constructor y se crea objeto con info del Tag que hizo la llamada.
            inventObject = self.InventoryActivity(self)           # "obj." es el objeto llamador, clase Tag
            print(f'####inventory - caller self = {self}  // ####inventory - Inventory Object: {inventObject} ')
            return inventObject   # Crea y retorna obj InventoryActivityAnimal y le asigna tagID pasado por "obj"

        def status(self):
            return self.StatusActivity(self)    # Crea y retorna obj StatusActivityAnimal y le asigna tagID pasado por "obj"

        class InventoryActivity(Activity):
            def __init__(self, outer):
                Activity.__init__(self)
                self.__outer = outer
                # print(f'CONSTR InventoryActivityAnimal - me llamo: {outer}')

            @property
            def inner_var(self):
                return self.__outer.tagID

            def set(self):
                print(f'I"m setting from Inner Classs Inventory Activity - inner_var(TagID): {self.inner_var} // outer_var: {self.__outer}')
                return self.inner_var

            def get(self):
                print(f'I"m getting from Inner Classs Inventory Activity')
                return self.inner_var

        class StatusActivity(Activity):
            def __init__(self, outer):
                Activity.__init__(self)
                self.__outer = outer
                # print(f'CONSTR STATUSActivity - me llamo: {outer}')

            @property
            def inner_var(self):
                return self.__outer.tagID

            def set(self):
                print(f'I"m setting from Inner Class Status. TagID: {self.inner_var} // outer_var: {self.__outer}')
                return self.inner_var

            def get(self):
                print(f'I AM getting from Inner Classs StatusActivity')
                return

            def isWhat(self):
                print(f'Es What? // self: {self}  // inner_var: {self.inner_var}')
                return self.inner_var

    tags = []
    for j in range(5):
        tags.append(Tag(j))     # Aqui se crean los tags SIN CREAR NINGUN OBJECTO InventoryActivityAnimal ni StatusActivityAnimal
        # inner_object.append(tags[j].__inventoryObj())      # Los objs InventoryActivityAnimal, StatusActivityAnimal se crean en cada llamada a __inventoryObj(), status()
                                                        # Dado que __inventoryObj(), status() son llamada siempre por un tag, se crea el objeto con la info del tag en __outerVar
    a = tags[0].inventory()     # Se crea 1 objeto de tipo ActivityInventory, con info de tag[0]
    c = tags[0].inventory()     # Se crea otro objeto de tipo ActivityInventory, con info de tag[0]
    print(f'LINEA INVENTORY - get_inner_object(a): {a}  // get_inner_object(c): {c} // InventoryActivity.set(c): {c.set()}')
    print(f'LINEA STATUS - get_inner_object: {tags[0].status()}   // Status.set(): {tags[0].status().set()}  // Status.isWhat()  {tags[0].status().isWhat()}')

# 1. Definir Activity fuera de Entity Object. No es necesario.
# 2. Crear 1 clase por cada actividad DENTRO de Tags. Lo mismo para Animales, Personas. Agregar @property en cada clase
# 3. En cada clase Activity, renombrar metodo  get_inner_object(obj) a __inventoryObj(obj), status(obj), etc.
# 4. Cada clase Activity debera definir Tablas y demas parametros comunes como class Attributes, a fines de eficiencia



                                            # CODIGO ORIGINAL
    class Outer(object):
        def __init__(self):
            self.outer_var = 1

        def get_inner_object(self):
            return self.Inner(self)     # Esta es la linea magica: obj. se pasa como parametro "outer" a Inner(obj) para setear el parametro obj.outer en el constructor Inner()

        class Inner(object):
            def __init__(self, outer):
                self.outer = outer      # outer es un objeto de clase Outer. Por eso se puede acceder como en linea 91.

            @property
            def inner_var(self):
                return self.outer.outer_var

    outer_object = Outer()
    inner_object = outer_object.get_inner_object()      # <__main__.Outer.Inner object at 0x03B6C490>   outer_object invoca a constructor Inner() indirectamente,
                                            # a traves de la llamada get_inner_object.        No se necesita este paso para pasar parametro tagID.
    print(inner_object.inner_var)                       # Solo se usa para imprimir

