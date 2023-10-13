import sqlite3

from krnl_sqlite import SQLiteQuery, getTblName, getFldName


qryObj = SQLiteQuery()


def redefine_fields_to_unique(tblName):

    cmd_list_Animales = [f' PRAGMA foreign_keys = off; ',
                f' BEGIN TRANSACTION; ',
                f' ALTER TABLE "{tblName}" RENAME TO "{"old_" + tblName}"; ',
                f' CREATE TABLE "{tblName}" (ID_Link INTEGER PRIMARY KEY, ID_Actividad INTEGER NOT NULL DEFAULT 0, '
                f' ID_Animal INTEGER NOT NULL DEFAULT 0, Ordinal De Secuencia INTEGER, Instancia Del Tratamiento INTEGER, '
                f' Comentario TEXT, PushUpload JSON, Bitmask INTEGER, TimeStamp TIMESTAMP,'
                f' UNIQUE (ID_Actividad, ID_Animal));',
                f' PRAGMA foreign_keys = on;']

    cmd_list_Caravanas = [f' PRAGMA foreign_keys = off; ',
                          f' BEGIN TRANSACTION; ',
                          f' ALTER TABLE "{tblName}" RENAME TO "{"old_" + tblName}"; ',
                          f' CREATE TABLE "{tblName}" (ID_Link INTEGER PRIMARY KEY, ID_Actividad INTEGER NOT NULL DEFAULT 0, '
                          f' ID_Caravana INTEGER NOT NULL DEFAULT 0, Comentario TEXT, PushUpload JSON, Bitmask INTEGER, '
                          f' TimeStamp TIMESTAMP,'
                          f' UNIQUE (ID_Actividad, ID_Caravana));',
                          f' PRAGMA foreign_keys = on;']                 #         f' COMMIT; ',

    copy_cmd = [f' PRAGMA foreign_keys = off; ',
                f' BEGIN TRANSACTION; ',
                f' INSERT INTO "{tblName}" SELECT * FROM "{"old_" + tblName}"; ',
                f' PRAGMA foreign_keys = on;']


    for j in cmd_list_Animales:
        print(f'{j}')
    for sql in cmd_list_Animales:
        try:
            print(f'cmd: {sql}')
            qryObj.execute(sql)
        except sqlite3.Error as e:
            print(f'Command error: {e}')
            break
    try:
        for sql in copy_cmd:
            print(f'cmd: {sql}')
            qryObj.execute(sql)
    except sqlite3.Error as e:
        print(f'Command error: {e}')

def rename_timeStamp_fields():

    tables_no_timeStamp = {'tblAnimalesAlimentoNombres': 'FechaHora Actividad',
                            'tblAnimalesActividadesProgramadasTriggers': 'FechaHora Actividad',
                            'tblCaravanasRegistroDeActividades': 'FechaHora Registro',
                            'tblDataTMBancosLinkCuentasPersonas': 'FechaHora Actividad',
                            'tblDispositivos': 'FechaHora Actividad',
                            'tblDispositivosRegistroDeActividades': 'FechaHora Registro',
                            'tblLinkAnimalesActividadesProgramadas': 'FechaHora Registro',
                            'tblRegistroDeNotificaciones': 'FechaHora Registro',
                            'tblGeoEntidades': 'FechaHora Registro',
                            'tblListas': 'FechaHora Registro',
                            'tblPersonas': 'FechaHora Actividad',
                            'tblPersonasRegistroDeActividades': 'FechaHora Registro',
                            'tblProyectos': 'FechaHora Actividad',
                            'tblDispositivosRegistroDatastream': 'FechaHora Registro',
                            'tblDataPersonasDisparadoresOperandos': 'FechaHora',
                            'tblDataPersonasDisparadoresParametros': 'FechaHora',
                            'tblSysRegistroDeSistema': 'FechaHora Registro',
                            'tblDispositivosIdentificadores': 'FechaHora',
                            'tblAnimalesRegistroDeActividades': 'FechaHora Registro',
                            'tblTMRegistroDeActividades': 'FechaHora Registro',
                            'tblListasRegistroDeActividades': 'FechaHora Registro',
                            'tblCaravanasRegistroDataStream': 'FechaHora Registro',
                            'tblDispositivosRegistroDeActividadesProgramadas': 'FechaHora Registro',
                            'tbAnimalesAPSecuencias': 'FechaHora Actividad',
                            'tblAnimalesRegistroDeActividadesProgramadas': 'FechaHora Registro',
                            }

    allTables = ['Actividades Status',
                    'Animales',
                    'Animales Actividades Nombres',
                    'Animales Alimento Nombres',
                    'Animales Categorias',
                    'Data Animales Disparadores De Actividades',
                    'Animales Marcas',
                    'Animales Parametros Nombres',
                    'Animales Razas',
                    'Animales Referencias Señas',
                    'Animales Registro De Actividades',
                    'Animales Actividades Programadas Triggers',
                    'Animales Resultados De Paricion',
                    'Animales Sanidad Actividades Del Tratamiento TEMPLATE',
                    'Animales Sanidad Aplicaciones Nombres - NO USADA',
                    'Animales Sanidad Curaciones Nombres',
                    'Animales Sanidad Forma',
                    'Animales Sanidad Requerimientos',
                    'Animales Sanidad Tratamientos',
                    'Animales Sanidad Tratamientos Nombres',
                    'Animales Sanidad Tratamientos Subcategorias',
                    'Animales Status',
                    'Animales Status De Posesion',
                    'Animales Clases',
                    'Animales Tipos De AltaBaja',
                    'Caravanas',
                    'Caravanas Actividades Nombres',
                    'Caravanas Formato',
                    'Caravanas Registro De Actividades',
                    'Caravanas Status',
                    'Caravanas Tecnologia',
                    'Caravanas Tipos',
                    'Colores',
                    'Contacto Telefonos',
                    'Contacto Telefonos Extension',
                    'Contacto WebRedes',
                    'Data Animales Actividad Alimentacion Dieta',
                    'Data Animales Actividad Alimentacion Ingesta',
                    'Data Animales Actividad Alta',
                    'Data Animales Actividad Baja',
                    'Data Animales Actividad Caravanas',
                    'Data Animales Actividad Castracion',
                    'Data Animales Actividad Destete',
                    'Data Animales Actividad Dispositivos',
                    'Data Animales Actividad Extraccion Semen',
                    'Data Animales Actividad Inseminacion',
                    'Data Animales Actividad Listas',
                    'Data Animales Actividad Localizacion',
                    'Data Animales Actividad Marca',
                    'Data Animales Actividad Movimientos',
                    'Data Animales Actividad Ordeñe',
                    'Data Animales Actividad Parametros Individuales',
                    'Data Animales Actividad Paricion',
                    'Data Animales Actividad Personas',
                    'Animales Mediciones Nombres',
                    'Data Animales Actividad Preñez',
                    'Data Animales Actividad Sanidad Aplicaciones',
                    'Data Animales Actividad Sanidad Cirugia',
                    'Data Animales Actividad Sanidad Curacion',
                    'Data Animales Actividad MoneyActivity',
                    'Data Animales Actividad Tacto',
                    'Data Animales Actividad Medicion',
                    'Data Animales Alimento Racion',
                    'Data Animales Categorias',
                    'Data Animales Status De Posesion',
                    'Data Animales Parametros Generales',
                    'Data Programacion De Actividades',
                    'Data Animales Actividad Servicios',
                    'Data Animales Señas Particulares',
                    'Data Caravanas Datos',
                    'Caravanas Identificadores Secundarios',
                    'Data Caravanas Localizacion',
                    'Data Caravanas Personas',
                    'Data Caravanas Reemplazo',
                    'Data Caravanas MoneyActivity',
                    'Data Dispositivos Listas',
                    'Data Dispositivos Localizacion',
                    'Data Dispositivos Personas',
                    'Animales Registro De Actividades Programadas',
                    'Data Dispositivos MoneyActivity',
                    'Data Geo Entidades Coordenadas',
                    'Data Listas Elementos',
                    'Sys Data Parametros Generales',
                    'Data Personas Datos',
                    'Data Personas Listas',
                    'Data Personas Localizacion',
                    'Animales AP Secuencias',
                    'Data Personas MoneyActivity',
                    'Data Personas Usuarios Datos',
                    'Data Proyectos Agricultura',
                    'Data Proyectos Animales',
                    'Data Proyectos Dispositivos',
                    'Data Proyectos Forecasting',
                    'Data Proyectos Listas',
                    'Data Proyectos Localizacion',
                    'Data Proyectos Personas',
                    'Data Proyectos MoneyActivity',
                    'Data MoneyActivity Bancos',
                    'Data MoneyActivity Bancos Calificacion y Limite De Credito',
                    'Data MoneyActivity Bancos Cuentas Bancarias',
                    'Data MoneyActivity Bancos Link CuentasPersonas',
                    'Data MoneyActivity Localizacion',
                    'Data MoneyActivity Monedas De Referencia',
                    'Data MoneyActivity Monedas Por Defecto',
                    'Data MoneyActivity Monedas Tasas De Cambio',
                    'Data MoneyActivity Montos',
                    'Data MoneyActivity Personas',
                    'Data MoneyActivity Transacciones',
                    'Dispositivos',
                    'Dispositivos Actividades Nombres',
                    'Dispositivos Registro De Actividades',
                    'Dispositivos Tipos',
                    'Dispositivos Vehiculos Tipos',
                    'Sys Errores Codigos',
                    'Explotacion Subtipos',
                    'Explotacion Tipos',
                    'Geo Departamentos - NO USADA',
                    'Geo Establecimientos - NO USADA',
                    'Geo Locaciones - NO USADA',
                    'Registro De Notificaciones',
                    'Animales Registros De Cria - RETIRED',
                    'Geo Niveles De Localizacion',
                    'Link Animales Actividades Programadas',
                    'Geo Entidad Container',
                    'Animales Senias Particulares',
                    'Geo Region Provincia - NO USADA',
                    'Geo Entidades',
                    'Insumos Y Productos',
                    'Insumos Y Productos Tipos',
                    'Link Animales AlimentosInsumos (Receta)',
                    'Link Personas Contacto Telefonos',
                    'Link Personas Contacto WebRedes',
                    'Listas',
                    'Materiales Peligrosos',
                    'Objetos Tipos De Transferencia De Propiedad',
                    'Personas',
                    'Personas Actividades Nombres',
                    'Personas Registro De Actividades',
                    'Personas Usuarios Niveles De Acceso',
                    'Personas Usuarios Status',
                    'Productos',
                    'Productos Tipos',
                    'Proyectos',
                    'Proyectos Registro De Actividades',
                    'Proyectos Status',
                    'Proyectos Tipos',
                    'Sitios Tipos',
                    'MoneyActivity Actividades Nombres',
                    'MoneyActivity Bancos Nombres',
                    'MoneyActivity Calificaciones De Credito',
                    'MoneyActivity Cuentas Tipos',
                    'MoneyActivity Monedas Nombres',
                    'MoneyActivity Registro De Actividades',
                    'Telefonos Tipos',
                    'Unidades Nombres',
                    'Unidades Sistemas',
                    'Unidades Tipos',
                    'Sys Versiones DB',
                    'Animales Actividad Sanidad Info Aplicaciones',
                    'Link Animales Actividades',
                    'Link Personas Actividades',
                    'Link Caravanas Actividades',
                    'Link Dispositivos Actividades',
                    'Data Personas Disparadores De Actividades',
                    'Dispositivos Registro Datastream',
                    'Tipos De Dato',
                    'Data Dispositivos Datastream',
                    'Sys Parametros Nombres',
                    'Data Personas Disparadores Operandos',
                    'Data Personas Disparadores Parametros',
                    'Data Animales Disparadores Parametros',
                    'Data Animales Disparadores Operandos',
                    'Caravanas Identificadores Secundarios Tipos De Marca',
                    'Caravanas Link Caravanas IS',
                    'Sys Registro De Sistema',
                    'Sys Actividades Nombres',
                    'Objetos Tipos',
                    'Objetos Tipos De UID',
                    'Dispositivos Identificadores',
                    'Dispositivos Status',
                    'Data Dispositivos Datos',
                    'Personas Status',
                    'Data Personas Direccion',
                    'Geo Ciudades Pueblos Lugares - NO USADA',
                    'Geo Entidades Codigos Postales',
                    'Data Dispositivos Caravanas',
                    'Data Dispositivos Inventario',
                    'Data Dispositivos Status',
                    'Data Caravanas Inventario',
                    'Data Caravanas Dispositivos',
                    'Data Caravanas Status',
                    'Data Animales Actividad Inventario',
                    'Data Personas Status',
                    'Data Personas Inventario',
                    'Personas Niveles',
                    'Data Animales Actividad Status',
                    'Tablas Data Actividades Animales',
                    'Data Personas Dispositivos',
                    'Objetos Clases',
                    'Data Listas MoneyActivity',
                    'Data Listas Items',
                    'Listas Registro De Actividades',
                    'Listas Status',
                    'Listas Actividades Nombres',
                    'Data Animales Actividad Items',
                    'Data Caravanas Items',
                    'Activos Clases',
                    'Data Personas Items',
                    'Data Caravanas Datastream',
                    'Caravanas Registro DataStream',
                    'Geo Tipos De Entidad',
                    'Data Animales Actividades Programadas Status',
                    'Dispositivos Registro De Actividades Programadas',
                    'Personas Registro De Actividades Programadas',
                    'Actividades Signatures',
                    ]

    rename_cmd1 = [f' PRAGMA foreign_keys = off; ',
                     f' BEGIN TRANSACTION; ']

                    # f' PRAGMA foreign_keys = on;' ]

    for sql in rename_cmd1:
        try:
            print(f'cmd: {sql}')
            qryObj.execute(sql)
        except sqlite3.Error as e:
            print(f'Command error: {e}')
            break
    for j in tables_no_timeStamp:
        sql = f'ALTER TABLE "{getTblName(j)}" ADD COLUMN "TimeStamp" TIMESTAMP; '
        try:
            print(f'cmd: {sql}')
            qryObj.execute(sql)
        except sqlite3.Error as e:
            print(f'Command error: {e}')
        finally:
            continue

    for j in allTables:
        sql = f'ALTER TABLE "{j}" RENAME COLUMN "TimeStamp" to "TimeStamp Sync"; '
        try:
            print(f'cmd: {sql}')
            qryObj.execute(sql)
        except sqlite3.Error as e:
            print(f'Command error: {e}')
        finally:
            continue



if __name__ == '__main__':
    pass

    # redefine_fields_to_unique('Link Animales Actividades')
    # rename_timeStamp_fields()
