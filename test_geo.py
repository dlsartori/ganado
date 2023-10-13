from krnl_tm import *
from krnl_cfg import *
from krnl_tag import *
from krnl_abstract_class_animal import *
from inspect import currentframe, getframeinfo
from krnl_db_access import writeObj
from krnl_geo import Geo


if __name__ == '__main__':
    sys.setswitchinterval(0.002)  # TODO: default=0.005. Vamo a ver como va...
    print(f'switchinterval is set to {sys.getswitchinterval()} seconds.')
    entity = 553            # Departamento 9 de Julio: 237 - El Nandu: 545 - El Nandu Lote 2, Potrero 1: 553
    a = handlerGeo.getContainerEntities(entity, mode=1)
    print(f'Container Entities for {entity}: {a}')

    # # Codigo creacion de Container Tree para cada registro en Geo Entidades
    # temp = getRecords('tblGeoEntidades', '', '', None, '*')       # Codigo p/ setear campo Container Tree
    # for j in range(temp.dataLen):
    #     temp.setVal(j, fldContainerTree=handlerGeo.getContainerEntities(temp.getVal(j, 'fldID'), mode=1))
    #     setRecord('tblGeoEntidades', **temp.unpackItem(j))

    # Creacion de Establecimiento
    handlerGeo.createGeoEntity(fldFK_TipoDeEntidad=40, fldName='El Carioca V', fldAbbreviation='EC-5',
                               fldFK_EntidadContainer=[237], fldFK_NivelDeLocalizacion=40)
    # Creacion de Lote
    handlerGeo.createGeoEntity(fldFK_TipoDeEntidad=50, fldName='El Carioca IV - Lote IX', fldAbbreviation='EC-4L9',
                               fldFK_EntidadContainer=[554], fldFK_NivelDeLocalizacion=50, fldFK_Establecimiento=554)
    # Creacion de Provincia
    handlerGeo.createGeoEntity(fldFK_TipoDeEntidad=20, fldName='Provincia 31', fldAbbreviation='P-31',
                               fldFK_EntidadContainer=10, fldFK_NivelDeLocalizacion=20)     #30 es error en fldFK_EntidadContainer

    # Creacion de Pais Nuevo
    handlerGeo.createGeoEntity(fldFK_TipoDeEntidad=10, fldName='Berretalandia', fldAbbreviation='BRTLND',
                               fldFK_NivelDeLocalizacion=10)

    writeObj.stop()



    def algo(*args):
        argsParsed = []
        for i in range(len(args)):
            if isinstance(args[i], int) and args[i] > 0:
                argsParsed.append(args[i])
        # print(f'len(args): {len(args)}')
        # print(f'len(argsParsed) = {len(argsParsed)}')
        # print(f'argsParsed: {argsParsed}')
        # iteratos = []
        # for i in args:
        #     if type(i) in [list, tuple, set]:
        #         iteratos.append(i)
        # print(f'Los Iteratos son: {iteratos}')
    # algo(4, 77, 'ab', (0, 1, 2), 93.3, -8, 1000, 0, ['NULL', 'NO NULL'], '', 99, None)
    # print(Person.getPersonLevels(1))
    # print(f'Nones Extended = {[*nones, 0, 1, 2]}')

    # numberDigits = '0123456789.'
    # stringui = 'El Ñandú Río Negré GÜEMES aquí'
    # print(f'{stringui} queda como: {removeAccents(stringui)}')        # Prueba funcionamiento de removeAccents()
    # str2 = 'Que tal 1 te fue 48.3 la pilcha no 567, 4 te qu,ed345,6 nada mal'
    # print(str2)
    # str3 = ''
    # for i in range(len(str2)):
    #     if str2[i].isnumeric() or str2[i] == ',':
    #         str3 += str2[i]
    # print(f'Y str3 es: {str3}')

    # __edadLimiteTernera = fetchAnimalParameterValue('Edad Limite Ternera')
    # __edadLimiteVaquillona = fetchAnimalParameterValue('Edad Limite Vaquillona')
    # __edadLimiteVaca = fetchAnimalParameterValue('Edad Limite Vaca')
    # __edadLimiteTernero = fetchAnimalParameterValue('Edad Limite Ternero')
    # __edadLimiteTorito = fetchAnimalParameterValue('Edad Limite Torito')
    # __edadLimiteNovillito = fetchAnimalParameterValue('Edad Limite Novillito')
    # __edadLimiteNovillo = fetchAnimalParameterValue('Edad Limite Novillo')
    # __edadLimiteToro = fetchAnimalParameterValue('Edad Limite Toro')
    #
    # print(f'Ternera: {__edadLimiteTernera} / Vaquillona: {__edadLimiteVaquillona} / Vaca: {__edadLimiteVaca} / '
    #       f'Ternero: {__edadLimiteTernero} / Torito:{__edadLimiteTorito} / Novillito: {__edadLimiteNovillito} / '
    #       f'Novillo: {__edadLimiteNovillo} / Toro: {__edadLimiteToro}')

