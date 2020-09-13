CREATE TABLE [Animales] (
    [ID_Animal] INTEGER PRIMARY KEY,
    [ID_Raza_Tipo De Animal] INTEGER,
    [Fecha De Nacimiento] TEXT NOT NULL,
    [Fecha Entrada en Servicio] TEXT,
    [Comentario Animal] TEXT
);
CREATE TABLE [Animales Actividades Programadas Nombres] (
    [ID_Nombre Actividad Programada] INTEGER,
    [Nombre Actividad] TEXT,
    [ID_Tipo De Animal] INTEGER,
    [Comentario Actividad] TEXT,
    PRIMARY KEY ([Nombre Actividad], [ID_Tipo De Animal])
);
CREATE TABLE [Animales Categorias] (
    [ID_Categoria] INTEGER,
    [ID_Tipo De Animal] INTEGER,
    [Nombre Categoria] TEXT,
    [MachoHembra] TEXT,
    [Castrado] INTEGER,
    PRIMARY KEY ([ID_Tipo De Animal], [Nombre Categoria])
);
CREATE TABLE [Animales Marcas] (
    [ID_Marca] INTEGER PRIMARY KEY,
    [ID_Tipo De Animal] INTEGER NOT NULL,
    [ID_Provincia] INTEGER NOT NULL,
    [ID_Persona] INTEGER NOT NULL,
    [Nombre Marca] TEXT NOT NULL,
    [Dibujo] BLOB,
    [Dato Boleto De Marca 1] TEXT,
    [Dato Boleto De Marca 2] TEXT,
    [Dato Boleto De Marca 3] TEXT
);
CREATE TABLE [Animales Razas] (
    [ID_Raza_Tipo De Animal] INTEGER,
    [ID_Tipo De Animal] INTEGER,
    [Nombre Raza] TEXT,
    [Meses Entrada En Servicio] INTEGER,
    [Comentario Raza] TEXT,
    PRIMARY KEY ([ID_Tipo De Animal], [Nombre Raza])
);
CREATE TABLE [Animales Sanidad Aplicaciones Nombres] (
    [ID_Sanidad Aplicacion] INTEGER PRIMARY KEY,
    [Nombre Aplicacion] TEXT NOT NULL,
    [Comentario Aplicacion] TEXT
);
CREATE TABLE [Animales Sanidad Curaciones Nombres] (
    [ID_Nombre De Curacion] INTEGER PRIMARY KEY,
    [Nombre De Curacion] TEXT,
    [Descripcion De Curacion] TEXT,
    [Comentario Curacion] TEXT
);
CREATE TABLE [Animales Sanidad Forma] (
    [ID_Forma De Sanidad] INTEGER PRIMARY KEY,
    [Nombre Forma De Sanidad] TEXT UNIQUE NOT NULL,
    [Comentario Forma De Sanidad] TEXT
);
CREATE TABLE [Animales Sanidad Requerimientos] (
    [ID_Requerimiento De Sanidad] INTEGER PRIMARY KEY,
    [Requerimiento De Sanidad] TEXT UNIQUE NOT NULL,
    [Comentario Requerimiento Sanidad] TEXT
);
CREATE TABLE [Animales Sanidad Status AplicacionActividad] (
    [ID_Status AplicacionActividad] INTEGER PRIMARY KEY,
    [Status AplicacionActividad] TEXT NOT NULL,
    [Comentario Status AplicacionActividad] TEXT
);
CREATE TABLE [Animales Sanidad Tamaño de Dosis] (
    [ID_Tamaño De Dosis] INTEGER PRIMARY KEY,
    [Tamaño De Dosis] TEXT NOT NULL,
    [Comentario Tamaño De Dosis] TEXT
);
CREATE TABLE [Animales Sanidad Tratamientos Nombres] (
    [ID_Nombre De Tratamiento] INTEGER PRIMARY KEY,
    [Nombre De Tratamiento] TEXT,
    [Descripcion De Tratamiento] TEXT,
    [Comentario Tratamiento] TEXT
);
CREATE TABLE [Animales Sanidad Tratamientos Subcategorias] (
    [ID_Tratamiento Subcategoria] INTEGER PRIMARY KEY,
    [Nombre Subcategoria] TEXT UNIQUE NOT NULL,
    [Comentario Subcategoria] TEXT
);
CREATE TABLE [Animales Status] (
    [ID_Status Animal] INTEGER PRIMARY KEY,
    [Status Animal] TEXT NOT NULL,
    [Activo] INTEGER,
    [Archivado] INTEGER,
    [Comentario Status Animal] TEXT
);
CREATE TABLE [Animales Tipos] (
    [ID_Tipo De Animal] INTEGER PRIMARY KEY,
    [Nombre Tipo De Animal] TEXT UNIQUE NOT NULL,
    [Comentario Tipo De Animal] TEXT
);
CREATE TABLE [Caravanas] (
    [ID_Caravana] INTEGER UNIQUE,
    [ID_Color] INTEGER,
    [Numero Caravana] TEXT,
    [ID_Tipo De Caravana] INTEGER,
    [Tecnologia Caravana] TEXT,
    [Formato Caravana] TEXT NOT NULL,
    [FechaHora Actividad] TEXT,
    [Comentario Caravana] TEXT,
    PRIMARY KEY ([ID_Color], [Numero Caravana], [ID_Tipo De Caravana], [Tecnologia Caravana])
);
CREATE TABLE [Caravanas Status] (
    [ID_Status Caravana] INTEGER,
    [Status Caravana] TEXT PRIMARY KEY,
    [Comentario Status Caravana] TEXT
);
CREATE TABLE [Caravanas Tipos] (
    [ID_Tipo De Caravana] INTEGER,
    [Tipo De Caravana] TEXT PRIMARY KEY,
    [Requerida Por Autoridad Sanitaria] INTEGER
);
CREATE TABLE [Colores] (
    [ID_Color] INTEGER PRIMARY KEY,
    [Nombre Color] TEXT NOT NULL,
    [RGBhex] TEXT NOT NULL
);
CREATE TABLE [Contacto Telefonos] (
    [ID_Telefono] INTEGER PRIMARY KEY,
    [Numero Telefonico] TEXT NOT NULL,
    [Tipo De Telefono] TEXT NOT NULL,
    [Mensajeria Chat] INTEGER,
    [Comentario Telefono] TEXT
);
CREATE TABLE [Contacto Telefonos Extension] (
    [ID_Telefono Extension] INTEGER PRIMARY KEY,
    [ID_Telefono] INTEGER,
    [Titular Extension] TEXT,
    [Comentario Extension] TEXT
);
CREATE TABLE [Contacto WebRedes] (
    [ID_Contacto WebRedes] INTEGER PRIMARY KEY,
    [Nombre WebRedes] TEXT NOT NULL,
    [Tipo De Sitio] TEXT NOT NULL,
    [Direccion WebRedes] TEXT,
    [Comentario WebRedes] TEXT
);
CREATE TABLE [Data Animales Actividades Programadas] (
    [ID_Data Actividad Programada] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [ID_Nombre Actividad Programada] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [ID_Data Localizacion] INTEGER,
    [ID_Nivel De Localizacion] INTEGER,
    [Fecha Actividad Programada] TEXT,
    [Fecha Actividad Cerrada] TEXT,
    [ID_Status AplicacionActividad Cierre] INTEGER,
    [Recurrencia (Dias)] INTEGER,
    [Comentario Actividad Programada] TEXT
);
CREATE TABLE [Data Animales Animales_Caravanas] (
    [ID_Data Animal_Caravana] INTEGER,
    [ID_Animal] INTEGER,
    [ID_Caravana] INTEGER,
    [FechaHora Actividad] TEXT,
    [ID_Persona Dueño] INTEGER,
    [ID_Data Costo Unitario] INTEGER,
    PRIMARY KEY ([ID_Animal], [ID_Caravana], [FechaHora Actividad])
);
CREATE TABLE [Data Animales Caravanas Reemplazadas] (
    [ID_Data Reemplazo De Caravana] INTEGER UNIQUE,
    [ID_Caravana Nueva] INTEGER,
    [FechaHora Actividad] TEXT,
    [ID_Caravana Reemplazada] INTEGER,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Reemplazo] TEXT,
    PRIMARY KEY ([ID_Caravana Nueva], [FechaHora Actividad], [ID_Caravana Reemplazada])
);
CREATE TABLE [Data Animales Categorias] (
    [ID_Data Categoria] INTEGER,
    [ID_Animal] INTEGER,
    [ID_Categoria] INTEGER,
    [FechaHora Actividad] TEXT,
    [Comentario Categoria] TEXT,
    PRIMARY KEY ([ID_Animal], [ID_Categoria], [FechaHora Actividad])
);
CREATE TABLE [Data Animales Categorias Tratamientos_Geografia] (
    [ID_Categoria Incluida Tratamientos] INTEGER,
    [ID_Tratamiento_Geografia] INTEGER,
    [ID_Categoria] INTEGER,
    [FechaHora Actividad] INTEGER NOT NULL,
    [Comentario Categoria Incluida] TEXT,
    PRIMARY KEY ([ID_Tratamiento_Geografia], [ID_Categoria])
);
CREATE TABLE [Data Animales Destete] (
    [ID_Data Destete] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Destete] TEXT
);
CREATE TABLE [Data Animales Dueño] (
    [ID_Data Dueño] INTEGER,
    [ID_Animal] INTEGER,
    [ID_Persona] INTEGER,
    [FechaHora Actividad] TEXT,
    [Comentario Dueño] TEXT,
    PRIMARY KEY ([ID_Animal], [ID_Persona], [FechaHora Actividad])
);
CREATE TABLE [Data Animales Entrada] (
    [ID_Data Entrada] INTEGER,
    [ID_Animal] INTEGER PRIMARY KEY,
    [FechaHora Actividad] TEXT NOT NULL,
    [Tipo De Entrada] TEXT NOT NULL,
    [ID_Animal Madre] INTEGER NOT NULL,
    [ID_Animal Padre] INTEGER,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Entrada] TEXT
);
CREATE TABLE [Data Animales Inventario] (
    [ID_Data Inventario] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Inventariado] INTEGER,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Inventario] TEXT
);
CREATE TABLE [Data Animales Localizacion] (
    [ID_Data Localizacion] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [ID_Lote] INTEGER NOT NULL,
    [ID_Potrero] INTEGER NOT NULL,
    [Comentario Localizacion] TEXT
);
CREATE TABLE [Data Animales Marca] (
    [ID_Data Marca] INTEGER,
    [ID_Animal] INTEGER,
    [ID_Marca] INTEGER,
    [FechaHora Actividad] TEXT,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Marca] TEXT,
    PRIMARY KEY ([ID_Animal], [ID_Marca], [FechaHora Actividad])
);
CREATE TABLE [Data Animales Movimientos Internos] (
    [ID_Data Movimiento] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [ID_Data Localizacion Origen] INTEGER NOT NULL,
    [ID_Lote Destino] INTEGER NOT NULL,
    [ID_Potrero Destino] INTEGER,
    [ID_Vehiculo] INTEGER,
    [ID_Status General] INTEGER,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Movimiento] TEXT
);
CREATE TABLE [Data Animales Parametros Generales] (
    [ID_Parametro Animal] INTEGER PRIMARY KEY,
    [FechaHora Actividad] TEXT,
    [ID_Tipo De Animal] INTEGER,
    [Nombre Parametro] TEXT,
    [Valor Parametro] INTEGER,
    [Comentario Parametro Animal] TEXT
);
CREATE TABLE [Data Animales Pariciones] (
    [ID_Data Paricion] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Resultado De Paricion] TEXT NOT NULL,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Paricion] TEXT
);
CREATE TABLE [Data Animales Pesaje] (
    [ID_Data Pesaje] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Peso] INTEGER NOT NULL,
    [ID_Unidad] INTEGER NOT NULL,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Pesaje] TEXT
);
CREATE TABLE [Data Animales Preñez] (
    [ID_Data Preñez] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [Preñez] INTEGER,
    [FechaHora Actividad] TEXT,
    [Fecha De Servicio] TEXT,
    [Comentario Preñez] TEXT
);
CREATE TABLE [Data Animales Salida] (
    [ID_Data Salida] INTEGER,
    [ID_Animal] INTEGER PRIMARY KEY,
    [FechaHora Actividad] TEXT NOT NULL,
    [Tipo De Salida] INTEGER NOT NULL,
    [ID_Data Costo Unitario] INTEGER,
    [ID_Data Precio Unitario] INTEGER,
    [Comentario Salida] TEXT
);
CREATE TABLE [Data Animales Sanidad Aplicaciones Programadas] (
    [ID_Data Aplicacion Programada] INTEGER,
    [ID_Animal] INTEGER,
    [ID_Link Tratamiento_Animal] INTEGER,
    [ID_Sanidad Aplicacion] INTEGER,
    [FechaHora Actividad] TEXT NOT NULL,
    [Fecha Programada] TEXT,
    [Dias Para Alerta] INTEGER,
    [ID_Tratamiento_Animal PLANTILLA] INTEGER,
    [Limite Dias Desde Fecha Programada] INTEGER,
    [ID_Data Aplicacion De Cierre] INTEGER,
    [ID_Status AplicacionActividad Cierre] INTEGER,
    [Comentario Aplicacion Programada] TEXT,
    PRIMARY KEY ([ID_Animal], [ID_Link Tratamiento_Animal], [ID_Sanidad Aplicacion], [Fecha Programada])
);
CREATE TABLE [Data Animales Sanidad Aplicaciones Realizadas] (
    [ID_Data Aplicacion] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [ID_Link Tratamiento_Animal] INTEGER NOT NULL,
    [ID_Sanidad Aplicacion] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [ID_Forma de Sanidad] INTEGER,
    [ID_Tamaño de Dosis] INTEGER,
    [Principio Activo] TEXT,
    [Nombre Producto] TEXT NOT NULL,
    [Concentracion (%)] INTEGER,
    [ID_Status AplicacionActividad] INTEGER NOT NULL,
    [ID_Data Costo Unitario] INTEGER,
    [Notificacion De Sanidad] TEXT,
    [Comentario Aplicacion] TEXT
);
CREATE TABLE [Data Animales Sanidad Castracion] (
    [ID_Data Castracion] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Castrado] INTEGER,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Castracion] TEXT
);
CREATE TABLE [Data Animales Sanidad Cirugia] (
    [ID_Data Cirugia] INTEGER,
    [ID_Animal] INTEGER,
    [Tipo de Cirugia] TEXT,
    [FechaHora Actividad] TEXT,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Cirugia] TEXT,
    PRIMARY KEY ([ID_Animal], [Tipo de Cirugia], [FechaHora Actividad])
);
CREATE TABLE [Data Animales Sanidad Cronograma Aplicaciones Animal Individual] (
    [ID_Cronograma Aplicacion Animal] INTEGER,
    [ID_Tratamiento_Animal PLANTILLA] INTEGER,
    [ID_Sanidad Aplicacion] INTEGER,
    [FechaHora Actividad] INTEGER NOT NULL,
    [Dias Hasta Aplicacion] INTEGER,
    [Comentario Cronograma de Aplicaciones] TEXT,
    PRIMARY KEY ([ID_Tratamiento_Animal PLANTILLA], [ID_Sanidad Aplicacion])
);
CREATE TABLE [Data Animales Sanidad Cronograma Aplicaciones Geografia] (
    [ID_Cronograma Aplicacion Geografia] INTEGER,
    [ID_Tratamiento_Geografia] INTEGER,
    [ID_Sanidad Aplicacion] INTEGER,
    [FechaHora Actividad] INTEGER NOT NULL,
    [Dias Hasta Aplicacion] INTEGER,
    [Comentario Cronograma de Aplicaciones] TEXT,
    PRIMARY KEY ([ID_Tratamiento_Geografia], [ID_Sanidad Aplicacion])
);
CREATE TABLE [Data Animales Sanidad Curacion] (
    [ID_Data Curacion] INTEGER,
    [ID_Animal] INTEGER,
    [ID_Nombre De Curacion] TEXT,
    [FechaHora Actividad] TEXT,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Curacion] TEXT,
    PRIMARY KEY ([ID_Animal], [ID_Nombre De Curacion], [FechaHora Actividad])
);
CREATE TABLE [Data Animales Sanidad Inseminacion] (
    [ID_Data Inseminacion] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Temperatura] INTEGER,
    [ID_Unidad Temperatura] INTEGER,
    [ID_Data Costo Unitario] INTEGER,
    [Otro Dato Inseminacion 1] TEXT,
    [Otro Dato Inseminacion 2] TEXT,
    [Comentario Inseminacion] TEXT
);
CREATE TABLE [Data Animales Sanidad Tacto] (
    [ID_Data Tacto] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Preñada] INTEGER,
    [Temperatura Tacto] INTEGER,
    [ID_Unidad] INTEGER,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Tacto] TEXT
);
CREATE TABLE [Data Animales Sanidad Temperatura] (
    [ID_Data Temperatura] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Temperatura] INTEGER NOT NULL,
    [ID_Unidad Temperatura] INTEGER,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Toma De Temperatura] TEXT
);
CREATE TABLE [Data Animales Señas Particulares] (
    [ID_Data Seña Particular] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Referencia Seña] TEXT,
    [Seña Particular] TEXT NOT NULL
);
CREATE TABLE [Data Animales Servicios] (
    [ID_Data Servicios] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Status Servicio] TEXT NOT NULL,
    [Inicio De Servicio] TEXT,
    [Fin De Servicio] TEXT,
    [ID_Data Costo Unitario] INTEGER,
    [Comentario Servicio] TEXT
);
CREATE TABLE [Data Animales Status] (
    [ID_Data Status] INTEGER,
    [ID_Animal] INTEGER,
    [FechaHora Actividad] TEXT,
    [ID_Status Animal] INTEGER NOT NULL,
    PRIMARY KEY ([ID_Animal], [FechaHora Actividad])
);
CREATE TABLE [Data Animales Ultimo Evento Registrado] (
    [ID_Data Ultimo Evento Registrado] INTEGER UNIQUE,
    [ID_Animal] INTEGER PRIMARY KEY,
    [FechaHora Actividad] TEXT NOT NULL
);
CREATE TABLE [Data Bancos Calificacion y Limite De Credito] (
    [ID_Data Calificacion y Limite De Credito] INTEGER PRIMARY KEY,
    [ID_Persona] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Calificacion De Credito] TEXT NOT NULL,
    [Monto Limite De Credito] INTEGER,
    [ID_Moneda] INTEGER,
    [Comentario Calificacion y Limite De Credito] TEXT
);
CREATE TABLE [Data Bancos Cuentas Bancarias] (
    [ID_Data Cuenta Bancaria] INTEGER,
    [Numero De Cuenta] TEXT,
    [Tipo De Cuenta] TEXT,
    [Nombre De Banco] TEXT,
    [FechaHora Actividad] TEXT,
    [ID_Moneda] INTEGER NOT NULL,
    [CBU] TEXT,
    [CBU Alias] TEXT,
    [Comentario Cuenta Bancaria] TEXT,
    PRIMARY KEY ([Numero De Cuenta], [Tipo De Cuenta], [Nombre De Banco], [FechaHora Actividad])
);
CREATE TABLE [Data Bancos Link_Cuentas_Personas] (
    [ID_Data Cuenta _Persona] INTEGER,
    [ID_Persona] INTEGER,
    [ID_Data Cuenta Bancaria] INTEGER,
    [FechaHora Actividad] TEXT,
    [Titular De Cuenta] TEXT,
    [Cuenta Activa] INTEGER,
    [Comentario Cuenta_Persona] TEXT,
    PRIMARY KEY ([ID_Persona], [ID_Data Cuenta Bancaria], [FechaHora Actividad])
);
CREATE TABLE [Data Caravanas Datos] (
    [ID_Data_Caravana Datos] INTEGER PRIMARY KEY,
    [ID_Caravana] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [RENSPA] TEXT,
    [ID_Data Costo Unitario] INTEGER,
    [ID_Status Caravana] INTEGER,
    [Comentario Caravana Datos] TEXT
);
CREATE TABLE [Data Caravanas Identificadores Secundarios] (
    [ID_Data Caravana Identificador Secundario] INTEGER,
    [ID_Caravana] INTEGER PRIMARY KEY,
    [FechaHora Actividad] TEXT NOT NULL,
    [Alfanumerico] TEXT,
    [Cantidad De PMC] TEXT,
    [Comentario Identificador Secundario] TEXT
);
CREATE TABLE [Data Establecimientos Parametros Tratamientos_Geografia] (
    [ID_Data Establecimiento Parametros Tratamiento] INTEGER,
    [ID_Establecimiento] INTEGER,
    [ID_Tratamiento_Geografia] INTEGER,
    [FechaHora Actividad] TEXT,
    [Fecha Aplicacion Inicial] TEXT,
    [Comentario Data Establecimiento Parametros Tratamiento] TEXT,
    PRIMARY KEY ([ID_Establecimiento], [ID_Tratamiento_Geografia], [FechaHora Actividad])
);
CREATE TABLE [Data Monedas De Referencia] (
    [ID_Data Moneda De Referencia] INTEGER,
    [ID_Moneda] INTEGER,
    [FechaHora Actividad] TEXT,
    [Comentario Moneda De Referencia] TEXT,
    PRIMARY KEY ([ID_Moneda], [FechaHora Actividad])
);
CREATE TABLE [Data Monedas Por Defecto] (
    [ID_Data Moneda Por Defecto] INTEGER,
    [ID_Pais] INTEGER,
    [FechaHora Actividad] TEXT,
    [ID_Moneda] INTEGER NOT NULL,
    [Comentario Moneda Por Defecto] TEXT,
    PRIMARY KEY ([ID_Pais], [FechaHora Actividad])
);
CREATE TABLE [Data Monedas Tasas de Cambio] (
    [ID_Data Tasa De Cambio] INTEGER,
    [ID_Data Moneda De Referencia] INTEGER,
    [ID_Moneda Transaccional] INTEGER,
    [FechaHora Actividad] TEXT,
    [Tasa De Cambio] INTEGER,
    [Comentario Tasa De Cambio] TEXT,
    PRIMARY KEY ([ID_Data Moneda De Referencia], [ID_Moneda Transaccional], [FechaHora Actividad])
);
CREATE TABLE [Data Paises_Unidades] (
    [ID_Data  Paises_Unidades] INTEGER,
    [ID_Pais] INTEGER,
    [ID_Unidad] INTEGER,
    [FechaHora Actividad] TEXT,
    PRIMARY KEY ([ID_Pais], [ID_Unidad], [FechaHora Actividad])
);
CREATE TABLE [Data Parametros Generales Otros] (
    [ID_Parametro] INTEGER PRIMARY KEY,
    [FechaHora Actividad] TEXT,
    [Nombre Parametro] TEXT,
    [Valor Parametro] TEXT,
    [Comentario Parametro] TEXT
);
CREATE TABLE [Data Personas Datos] (
    [ID_Data Personas Datos] INTEGER PRIMARY KEY,
    [ID_Persona] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Dueño] INTEGER,
    [Direccion] TEXT,
    [Ciudad] TEXT,
    [Codigo Postal] TEXT,
    [Provincia] TEXT,
    [Pais] TEXT,
    [ID_Status General] INTEGER,
    [Situacion IVA] TEXT,
    [Persona Campo Adicional 1] TEXT,
    [Persona Campo Adicional 2] TEXT,
    [Persona Campo Adicional 3] TEXT,
    [Persona Campo Adicional 4] TEXT
);
CREATE TABLE [Data TM Costos Unitarios] (
    [ID_Data Costo Unitario] INTEGER PRIMARY KEY,
    [ID_Data Transacccion Monetaria] INTEGER,
    [FechaHora Actividad] TEXT,
    [Monto] INTEGER NOT NULL,
    [ID_Moneda] INTEGER NOT NULL,
    [ID_Data Tasa De Cambio] INTEGER,
    [Comentario Costo Unitario] TEXT
);
CREATE TABLE [Data TM Precios Unitarios] (
    [ID_Data Precio Unitario] INTEGER PRIMARY KEY,
    [ID_Data Transacccion Monetaria] INTEGER,
    [FechaHora Actividad] TEXT,
    [Monto] INTEGER NOT NULL,
    [ID_Moneda] INTEGER NOT NULL,
    [ID_Data Tasa De Cambio] INTEGER,
    [Comentario Precio Unitario] TEXT
);
CREATE TABLE [Data TM Transacciones Monetarias] (
    [ID_Data Transaccion Monetaria] INTEGER PRIMARY KEY,
    [FechaHora Actividad] TEXT,
    [Monto] INTEGER NOT NULL,
    [Concepto] TEXT,
    [ID_Moneda] INTEGER NOT NULL,
    [Tipo De Transaccion] TEXT NOT NULL,
    [ID_Data Tasa De Cambio] INTEGER,
    [Comentario Transaccion Monetaria] TEXT
);
CREATE TABLE [Data Vehiculos Datos] (
    [ID_Data Vehiculos Datos] INTEGER PRIMARY KEY,
    [ID_Vehiculo] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [ID_Persona] INTEGER,
    [ID_Status General] INTEGER,
    [ID_Data Transaccion Monetaria] INTEGER,
    [Dato Adicional Vehiculo 1] TEXT,
    [Dato Adicional Vehiculo 2] TEXT,
    [Dato Adicional Vehiculo 3] TEXT,
    [Comentario Datos Vehiculo] TEXT
);
CREATE TABLE [Errores Codigos] (
    [ID_Codigo Error] INTEGER,
    [Numero Codigo Error] INTEGER PRIMARY KEY,
    [Descripcion Codigo Error] TEXT,
    [Mensaje De Error] TEXT,
    [Comentario Errores Codigos] TEXT
);
CREATE TABLE [Explotacion Subtipos] (
    [ID_Explotacion Subtipo] INTEGER PRIMARY KEY,
    [ID_Explotacion Tipo] INTEGER NOT NULL,
    [SubTipo De Explotacion] TEXT
);
CREATE TABLE [Explotacion Tipos] (
    [ID_ Explotacion Tipo] INTEGER,
    [Tipo De Explotacion] TEXT PRIMARY KEY,
    [Comentario Explotacion] TEXT
);
CREATE TABLE [Geo Departamentos] (
    [ID_Departamento] INTEGER,
    [ID_Provincia] REAL,
    [Nombre Departamento] TEXT,
    [Cabecera Departamento] TEXT,
    [ID_Nivel De Localizacion] REAL,
    [Comentario Departamento] TEXT,
    PRIMARY KEY ([ID_Provincia], [Nombre Departamento])
);
CREATE TABLE [Geo Establecimientos] (
    [ID_Establecimiento] INTEGER PRIMARY KEY,
    [Nombre Establecimiento] TEXT NOT NULL,
    [Coordenadas Establecimiento Latitud] INTEGER,
    [Coordenadas Establecimiento Longitud] INTEGER,
    [Superfice] INTEGER,
    [ID_Unidad] INTEGER,
    [ID_Explotacion Tipo] INTEGER,
    [ID_Nivel De Localizacion] INTEGER,
    [Comentario Establecimiento] TEXT
);
CREATE TABLE [Geo Establecimientos Locaciones] (
    [ID_Locacion] INTEGER,
    [Nombre Locacion] TEXT NOT NULL,
    [ID_Lote] INTEGER NOT NULL,
    [Parametro Locacion 1] TEXT,
    [Parametro Locacion 2] TEXT,
    [Parametro Locacion 3] TEXT,
    [Coordenadas Locacion Latitud] INTEGER,
    [Coordenadas Locacion Longitud] INTEGER,
    [ID_Nivel De Localizacion] INTEGER,
    [Comentario Locacion] TEXT
);
CREATE TABLE [Geo Establecimientos Lotes] (
    [ID_Lote] INTEGER PRIMARY KEY,
    [Nombre Lote] TEXT NOT NULL,
    [ID_Establecimiento] INTEGER NOT NULL,
    [ID_Provincia] INTEGER NOT NULL,
    [ID_Explotacion Subtipo] INTEGER NOT NULL,
    [Superficie Lote] INTEGER,
    [ID_Unidad] INTEGER,
    [Coordenadas Lote Latitud] INTEGER,
    [Coordenadas Lote Longitud] INTEGER,
    [ID_Nivel De Localizacion] INTEGER,
    [Comentario Lote] TEXT
);
CREATE TABLE [Geo Establecimientos Potreros] (
    [ID_Potrero] INTEGER,
    [Nombre Potrero] TEXT,
    [ID_Lote] INTEGER,
    [ID_Departamento] INTEGER,
    [Coordenadas Potrero Latitud] INTEGER,
    [Coordenadas Potrero Longitud] INTEGER,
    [Superficie Potrero] INTEGER,
    [ID_Unidad] INTEGER,
    [ID_Nivel De Localizacion] INTEGER,
    [Comentario Potrero] TEXT,
    PRIMARY KEY ([Nombre Potrero], [ID_Lote], [ID_Departamento])
);
CREATE TABLE [Geo Link Establecimientos_Provincias] (
    [ID_Link Establecimientos_Provincias] INTEGER UNIQUE,
    [ID_Establecimiento] INTEGER,
    [ID_Provincia] INTEGER,
    [ID_Nivel De Localizacion] INTEGER,
    [Comentario Establecimiento_Provincia] TEXT,
    PRIMARY KEY ([ID_Establecimiento], [ID_Provincia])
);
CREATE TABLE [Geo Link Lotes_Departamentos] (
    [ID_Link Lote_Departamento] INTEGER UNIQUE,
    [ID_Lote] INTEGER,
    [ID_Departamento] INTEGER,
    [ID_Nivel De Localizacion] INTEGER,
    [Comentario Lote_Departamento] TEXT,
    PRIMARY KEY ([ID_Lote], [ID_Departamento])
);
CREATE TABLE [Geo Niveles De Localizacion] (
    [ID_Nivel De Localizacion] INTEGER PRIMARY KEY,
    [Nombre Localizacion] TEXT NOT NULL,
    [Orden de Localizacion] INTEGER,
    [Nombre Tabla Geo Asociada] TEXT,
    [Localizacion Activa] INTEGER,
    [Comentario Nivel De Localizacion] TEXT
);
CREATE TABLE [Geo Paises] (
    [ID_Pais] INTEGER UNIQUE,
    [Nombre Pais] TEXT PRIMARY KEY,
    [Abreviacion Pais] TEXT,
    [ID_Nivel de Localizacion] INTEGER,
    [Capital Pais] TEXT
);
CREATE TABLE [Geo Provincias] (
    [ID_Provincia] INTEGER UNIQUE,
    [ID_Pais] INTEGER,
    [Nombre Provincia] TEXT,
    [Abreviacion] TEXT NOT NULL,
    [ID_Nivel De Localizacion] INTEGER,
    [Capital Provincia] TEXT,
    PRIMARY KEY ([ID_Pais], [Nombre Provincia])
);
CREATE TABLE [Geo Region Pais] (
    [ID_Region Pais] INTEGER UNIQUE,
    [ID_Pais] INTEGER,
    [Nombre Region Pais] TEXT,
    [Abreviacion] TEXT NOT NULL,
    [ID_Nivel De Localizacion] INTEGER,
    [Comentario Region Pais] TEXT,
    PRIMARY KEY ([ID_Pais], [Nombre Region Pais])
);
CREATE TABLE [Geo Region Provincia] (
    [ID_Region Provincia] INTEGER,
    [ID_Provincia] INTEGER,
    [Nombre Region Provincia] TEXT,
    [Abreviacion] TEXT NOT NULL,
    [ID_Nivel De Localizacion] INTEGER,
    [Comentario Region Provincia] TEXT,
    PRIMARY KEY ([ID_Provincia], [Nombre Region Provincia])
);
CREATE TABLE [Link Actividades Programdas_Geografia PLANTILLA] (
    [ID_Actividad Programada_Geografia] INTEGER PRIMARY KEY,
    [ID_Nombre Actividad Programada] INTEGER NOT NULL,
    [ID_Nivel De Localizacion Requerido] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [ID_Provincia] INTEGER NOT NULL,
    [ID_Departamento] INTEGER,
    [ID_Establecimiento] INTEGER NOT NULL,
    [ID_Lote] INTEGER NOT NULL,
    [Fecha De Inicio Actividad] INTEGER NOT NULL,
    [Recurrencia (Dias)] INTEGER,
    [Programada para M/H] TEXT,
    [Programada para Categorias] TEXT,
    [Comentario Actividad Programada_Geografia] TEXT
);
CREATE TABLE [Link Personas_Contacto Telefonos] (
    [ID_Link Persona_Telefonos] INTEGER,
    [ID_Telefono] INTEGER,
    [ID_Persona] INTEGER,
    [Comentario Persona_Telefono] TEXT,
    PRIMARY KEY ([ID_Persona])
);
CREATE TABLE [Link Personas_Contacto WebRedes] (
    [ID_Link Persona_Contacto WebRedes] INTEGER,
    [ID_Contacto WebRedes] INTEGER,
    [ID_Persona] INTEGER,
    [Comentario Link Persona_WebRedes] TEXT,
    PRIMARY KEY ([ID_Persona])
);
CREATE TABLE [Link Raza_Categoria] (
    [ID_Link Raza_Categoria] INTEGER,
    [ID_Categoria] INTEGER,
    [ID_Raza_Tipo De Animal] INTEGER,
    [Dias Inicio Categoria] INTEGER,
    [Dias Final Categoria] INTEGER,
    PRIMARY KEY ([ID_Raza_Tipo De Animal])
);
CREATE TABLE [Link SP Tratamientos_Animales PLANTILLA] (
    [ID_Tratamiento_Animal PLANTILLA] INTEGER PRIMARY KEY,
    [ID_Animal] INTEGER NOT NULL,
    [ID_Link Tratamiento_Animal] INTEGER NOT NULL,
    [ID_Tratamiento_Geografia] INTEGER,
    [FechaHora Actividad] TEXT NOT NULL,
    [ID_Data Localizacion] INTEGER,
    [Tratamiento OnOff] INTEGER,
    [ID_Requerimiento De Sanidad] INTEGER,
    [Recurrencia (Dias)] INTEGER,
    [Fecha Aplicacion Inicial] TEXT,
    [ID_Data Actividad Programada] INTEGER,
    [Dias Desde La Fecha Actividad] INTEGER,
    [Tratamiento Disparado Por Sistema] INTEGER,
    [Comentario Tratamiento Animal Individual] TEXT
);
CREATE TABLE [Link SP Tratamientos_Geografia PLANTILLA] (
    [ID_Tratamiento_Geografia] INTEGER,
    [ID_Link Tratamiento_Animal] INTEGER,
    [ID_Nivel De Localizacion] INTEGER,
    [FechaHora Actividad] TEXT,
    [ID_Pais] INTEGER,
    [ID_Provincia] INTEGER NOT NULL,
    [ID_Departamento] INTEGER,
    [ID_Establecimiento] INTEGER NOT NULL,
    [ID_Lote] INTEGER NOT NULL,
    [ID_Requerimiento De Sanidad] INTEGER NOT NULL,
    [Recurrencia (Dias)] INTEGER,
    [ID_Nombre Actividad Programada] INTEGER,
    [Tratamiento OnOff] INTEGER,
    [Comentario Tratamiento_Geografia] TEXT,
    PRIMARY KEY ([ID_Link Tratamiento_Animal], [ID_Nivel De Localizacion], [FechaHora Actividad])
);
CREATE TABLE [Link SP Tratamientos_Tipos de Animal] (
    [ID_Link Tratamiento_Animal] INTEGER,
    [ID_Tipo De Animal] INTEGER,
    [ID_Nombre De Tratamiento] INTEGER,
    [ID_Tratamiento Subcategoria] INTEGER,
    [Comentario Tratamiento_Animal] TEXT,
    PRIMARY KEY ([ID_Tipo De Animal], [ID_Nombre De Tratamiento], [ID_Tratamiento Subcategoria])
);
CREATE TABLE [Monedas Lista] (
    [ID_Moneda] INTEGER PRIMARY KEY,
    [Nombre Moneda] TEXT UNIQUE NOT NULL,
    [Abreviacion Moneda] TEXT UNIQUE,
    [Simbolo Moneda] TEXT,
    [Comentario Moneda] TEXT
);
CREATE TABLE [Personas] (
    [ID_Persona] INTEGER PRIMARY KEY,
    [Nombre o Razón Social] TEXT NOT NULL,
    [Apellidos] TEXT,
    [DNI] TEXT UNIQUE,
    [CUIT_CUIL] TEXT UNIQUE,
    [CUIG] TEXT UNIQUE,
    [RENSPA] TEXT,
    [Persona Campo Adicional 1] TEXT,
    [Persona Campo Adicional 2] TEXT,
    [Persona Campo Adicional 3] TEXT,
    [Comentario Persona] TEXT
);
CREATE TABLE [Status General] (
    [ID_Status General] INTEGER,
    [Status Operacion] TEXT PRIMARY KEY,
    [Descripcion Status General] TEXT
);
CREATE TABLE [Unidades Lista] (
    [ID_Unidad] INTEGER PRIMARY KEY,
    [Nombre Unidad] TEXT NOT NULL,
    [Tipo Unidad] TEXT NOT NULL,
    [Sigla] TEXT NOT NULL,
    [ID_Sistema De Unidades] INTEGER,
    [Comentario Unidad] TEXT
);
CREATE TABLE [Unidades Sistemas] (
    [ID_Sistema De Unidades] INTEGER UNIQUE,
    [Nombre Sistema De Unidades] TEXT PRIMARY KEY,
    [Comentario Sistema De Unidades] TEXT
);
CREATE TABLE [Usuarios] (
    [ID_Usuario] INTEGER PRIMARY KEY,
    [ID_Persona] INTEGER NOT NULL,
    [FechaHora Actividad] TEXT NOT NULL,
    [Login Usuario] TEXT,
    [Clave Usuario] TEXT,
    [ID_Nivel De Acceso] INTEGER,
    [ID_Status Usuario] INTEGER,
    [Comentario Usuario] TEXT
);
CREATE TABLE [Usuarios Niveles De Acceso] (
    [ID_Nivel De Acceso] INTEGER,
    [Nivel de Acceso] INTEGER PRIMARY KEY,
    [Nombre Nivel] TEXT NOT NULL,
    [Comentario Nivel] TEXT
);
CREATE TABLE [Usuarios Status] (
    [ID_Status Usuario] INTEGER,
    [Status Usuario] TEXT PRIMARY KEY,
    [Comentario Status Usuario] TEXT
);
CREATE TABLE [Vehiculos] (
    [ID_Vehiculo] INTEGER PRIMARY KEY,
    [Placa Vehiculo] TEXT UNIQUE NOT NULL,
    [Tipo De Vehiculo] TEXT,
    [Marca Vehiculo] TEXT,
    [Modelo Vehiculo] TEXT,
    [Fecha Entrada En Servicio] TEXT,
    [Numero Unidad] TEXT,
    [Alias Unidad] TEXT,
    [Otro Dato Vehiculo 1] TEXT,
    [Otro Dato Vehiculo 2] TEXT,
    [Comentario Vehiculo] TEXT
);
