CREATE TABLE Animales (
    ID_Animal PRIMARY KEY,
    ID_Raza_Tipo De Animal,
    Fecha De Nacimiento,
    Fecha Entrada en Servicio,
    Comentario Animal,
);
CREATE TABLE Animales Actividades Programadas Nombres (
    ID_Nombre Actividad Programada,
    Nombre Actividad PRIMARY KEY,
    ID_Tipo De Animal PRIMARY KEY,
    Comentario Actividad,
);
CREATE TABLE Animales Categorias (
    ID_Categoria,
    ID_Tipo De Animal PRIMARY KEY,
    Nombre Categoria PRIMARY KEY,
    MachoHembra,
    Castrado,
);
CREATE TABLE Animales Marcas (
    ID_Marca PRIMARY KEY,
    ID_Tipo De Animal,
    ID_Provincia,
    ID_Persona,
    Nombre Marca,
    Dibujo,
    Dato Boleto De Marca 1,
    Dato Boleto De Marca 2,
    Dato Boleto De Marca 3,
);
CREATE TABLE Animales Razas (
    ID_Raza_Tipo De Animal,
    ID_Tipo De Animal PRIMARY KEY,
    Nombre Raza PRIMARY KEY,
    Meses Entrada En Servicio,
    Comentario Raza,
);
CREATE TABLE Animales Sanidad Aplicaciones Nombres (
    ID_Sanidad Aplicacion PRIMARY KEY,
    Nombre Aplicacion,
    Comentario Aplicacion,
);
CREATE TABLE Animales Sanidad Curaciones Nombres (
    ID_Nombre De Curacion PRIMARY KEY,
    Nombre De Curacion,
    Descripcion De Curacion,
    Comentario Curacion,
);
CREATE TABLE Animales Sanidad Forma (
    ID_Forma De Sanidad PRIMARY KEY,
    Nombre Forma De Sanidad,
    Comentario Forma De Sanidad,
);
CREATE TABLE Animales Sanidad Requerimientos (
    ID_Requerimiento De Sanidad PRIMARY KEY,
    Requerimiento De Sanidad,
    Comentario Requerimiento Sanidad,
);
CREATE TABLE Animales Sanidad Status AplicacionActividad (
    ID_Status AplicacionActividad PRIMARY KEY,
    Status AplicacionActividad,
    Comentario Status AplicacionActividad,
);
CREATE TABLE Animales Sanidad Tamaño de Dosis (
    ID_Tamaño De Dosis PRIMARY KEY,
    Tamaño De Dosis,
    Comentario Tamaño De Dosis,
);
CREATE TABLE Animales Sanidad Tratamientos Nombres (
    ID_Nombre De Tratamiento PRIMARY KEY,
    Nombre De Tratamiento,
    Descripcion De Tratamiento,
    Comentario Tratamiento,
);
CREATE TABLE Animales Sanidad Tratamientos Subcategorias (
    ID_Tratamiento Subcategoria PRIMARY KEY,
    Nombre Subcategoria,
    Comentario Subcategoria,
);
CREATE TABLE Animales Status (
    ID_Status Animal PRIMARY KEY,
    Status Animal,
    Activo,
    Archivado,
    Comentario Status Animal,
);
CREATE TABLE Animales Tipos (
    ID_Tipo De Animal PRIMARY KEY,
    Nombre Tipo De Animal,
    Comentario Tipo De Animal,
);
CREATE TABLE Caravanas (
    ID_Caravana,
    ID_Color PRIMARY KEY,
    Numero Caravana PRIMARY KEY,
    ID_Tipo De Caravana PRIMARY KEY,
    Tecnologia Caravana PRIMARY KEY,
    Formato Caravana,
    FechaHora Actividad,
    Comentario Caravana,
);
CREATE TABLE Caravanas Status (
    ID_Status Caravana,
    Status Caravana PRIMARY KEY,
    Comentario Status Caravana,
);
CREATE TABLE Caravanas Tipos (
    ID_Tipo De Caravana,
    Tipo De Caravana PRIMARY KEY,
    Requerida Por Autoridad Sanitaria,
);
CREATE TABLE Colores (
    ID_Color PRIMARY KEY,
    Nombre Color,
    RGBhex,
);
CREATE TABLE Contacto Telefonos (
    ID_Telefono PRIMARY KEY,
    Numero Telefonico,
    Tipo De Telefono,
    Mensajeria Chat,
    Comentario Telefono,
);
CREATE TABLE Contacto Telefonos Extension (
    ID_Telefono Extension PRIMARY KEY,
    ID_Telefono,
    Titular Extension,
    Comentario Extension,
);
CREATE TABLE Contacto WebRedes (
    ID_Contacto WebRedes PRIMARY KEY,
    Nombre WebRedes,
    Tipo De Sitio,
    Direccion WebRedes,
    Comentario WebRedes,
);
CREATE TABLE Data Animales Actividades Programadas (
    ID_Data Actividad Programada PRIMARY KEY,
    ID_Animal,
    ID_Nombre Actividad Programada,
    FechaHora Actividad,
    ID_Data Localizacion,
    ID_Nivel De Localizacion,
    Fecha Actividad Programada,
    Fecha Actividad Cerrada,
    ID_Status AplicacionActividad Cierre,
    Recurrencia (Dias),
    Comentario Actividad Programada,
);
CREATE TABLE Data Animales Animales_Caravanas (
    ID_Data Animal_Caravana,
    ID_Animal PRIMARY KEY,
    ID_Caravana PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    ID_Persona Dueño,
    ID_Data Costo Unitario,
);
CREATE TABLE Data Animales Caravanas Reemplazadas (
    ID_Data Reemplazo De Caravana,
    ID_Caravana Nueva PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    ID_Caravana Reemplazada PRIMARY KEY,
    ID_Data Costo Unitario,
    Comentario Reemplazo,
);
CREATE TABLE Data Animales Categorias (
    ID_Data Categoria,
    ID_Animal PRIMARY KEY,
    ID_Categoria PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    Comentario Categoria,
);
CREATE TABLE Data Animales Categorias Tratamientos_Geografia (
    ID_Categoria Incluida Tratamientos,
    ID_Tratamiento_Geografia PRIMARY KEY,
    ID_Categoria PRIMARY KEY,
    FechaHora Actividad,
    Comentario Categoria Incluida,
);
CREATE TABLE Data Animales Destete (
    ID_Data Destete PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    ID_Data Costo Unitario,
    Comentario Destete,
);
CREATE TABLE Data Animales Dueño (
    ID_Data Dueño,
    ID_Animal PRIMARY KEY,
    ID_Persona PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    Comentario Dueño,
);
CREATE TABLE Data Animales Entrada (
    ID_Data Entrada,
    ID_Animal PRIMARY KEY,
    FechaHora Actividad,
    Tipo De Entrada,
    ID_Animal Madre,
    ID_Animal Padre,
    ID_Data Costo Unitario,
    Comentario Entrada,
);
CREATE TABLE Data Animales Inventario (
    ID_Data Inventario PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    Inventariado,
    ID_Data Costo Unitario,
    Comentario Inventario,
);
CREATE TABLE Data Animales Localizacion (
    ID_Data Localizacion PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    ID_Lote,
    ID_Potrero,
    Comentario Localizacion,
);
CREATE TABLE Data Animales Marca (
    ID_Data Marca,
    ID_Animal PRIMARY KEY,
    ID_Marca PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    ID_Data Costo Unitario,
    Comentario Marca,
);
CREATE TABLE Data Animales Movimientos Internos (
    ID_Data Movimiento PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    ID_Data Localizacion Origen,
    ID_Lote Destino,
    ID_Potrero Destino,
    ID_Vehiculo,
    ID_Status General,
    ID_Data Costo Unitario,
    Comentario Movimiento,
);
CREATE TABLE Data Animales Parametros Generales (
    ID_Parametro Animal PRIMARY KEY,
    FechaHora Actividad,
    ID_Tipo De Animal,
    Nombre Parametro,
    Valor Parametro,
    Comentario Parametro Animal,
);
CREATE TABLE Data Animales Pariciones (
    ID_Data Paricion PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    Resultado De Paricion,
    ID_Data Costo Unitario,
    Comentario Paricion,
);
CREATE TABLE Data Animales Pesaje (
    ID_Data Pesaje PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    Peso,
    ID_Unidad,
    ID_Data Costo Unitario,
    Comentario Pesaje,
);
CREATE TABLE Data Animales Preñez (
    ID_Data Preñez PRIMARY KEY,
    ID_Animal,
    Preñez,
    FechaHora Actividad,
    Fecha De Servicio,
    Comentario Preñez,
);
CREATE TABLE Data Animales Salida (
    ID_Data Salida,
    ID_Animal PRIMARY KEY,
    FechaHora Actividad,
    Tipo De Salida,
    ID_Data Costo Unitario,
    ID_Data Precio Unitario,
    Comentario Salida,
);
CREATE TABLE Data Animales Sanidad Aplicaciones Programadas (
    ID_Data Aplicacion Programada,
    ID_Animal PRIMARY KEY,
    ID_Link Tratamiento_Animal PRIMARY KEY,
    ID_Sanidad Aplicacion PRIMARY KEY,
    FechaHora Actividad,
    Fecha Programada PRIMARY KEY,
    Dias Para Alerta,
    ID_Tratamiento_Animal PLANTILLA,
    Limite Dias Desde Fecha Programada,
    ID_Data Aplicacion De Cierre,
    ID_Status AplicacionActividad Cierre,
    Comentario Aplicacion Programada,
);
CREATE TABLE Data Animales Sanidad Aplicaciones Realizadas (
    ID_Data Aplicacion PRIMARY KEY,
    ID_Animal,
    ID_Link Tratamiento_Animal,
    ID_Sanidad Aplicacion,
    FechaHora Actividad,
    ID_Forma de Sanidad,
    ID_Tamaño de Dosis,
    Principio Activo,
    Nombre Producto,
    Concentracion (%),
    ID_Status AplicacionActividad,
    ID_Data Costo Unitario,
    Notificacion De Sanidad,
    Comentario Aplicacion,
);
CREATE TABLE Data Animales Sanidad Castracion (
    ID_Data Castracion PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    Castrado,
    ID_Data Costo Unitario,
    Comentario Castracion,
);
CREATE TABLE Data Animales Sanidad Cirugia (
    ID_Data Cirugia,
    ID_Animal PRIMARY KEY,
    Tipo de Cirugia PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    ID_Data Costo Unitario,
    Comentario Cirugia,
);
CREATE TABLE Data Animales Sanidad Cronograma Aplicaciones Animal Individual (
    ID_Cronograma Aplicacion Animal,
    ID_Tratamiento_Animal PLANTILLA PRIMARY KEY,
    ID_Sanidad Aplicacion PRIMARY KEY,
    FechaHora Actividad,
    Dias Hasta Aplicacion,
    Comentario Cronograma de Aplicaciones,
);
CREATE TABLE Data Animales Sanidad Cronograma Aplicaciones Geografia (
    ID_Cronograma Aplicacion Geografia,
    ID_Tratamiento_Geografia PRIMARY KEY,
    ID_Sanidad Aplicacion PRIMARY KEY,
    FechaHora Actividad,
    Dias Hasta Aplicacion,
    Comentario Cronograma de Aplicaciones,
);
CREATE TABLE Data Animales Sanidad Curacion (
    ID_Data Curacion,
    ID_Animal PRIMARY KEY,
    ID_Nombre De Curacion PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    ID_Data Costo Unitario,
    Comentario Curacion,
);
CREATE TABLE Data Animales Sanidad Inseminacion (
    ID_Data Inseminacion PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    Temperatura,
    ID_Unidad Temperatura,
    ID_Data Costo Unitario,
    Otro Dato Inseminacion 1,
    Otro Dato Inseminacion 2,
    Comentario Inseminacion,
);
CREATE TABLE Data Animales Sanidad Tacto (
    ID_Data Tacto PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    Preñada,
    Temperatura Tacto,
    ID_Unidad,
    ID_Data Costo Unitario,
    Comentario Tacto,
);
CREATE TABLE Data Animales Sanidad Temperatura (
    ID_Data Temperatura PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    Temperatura,
    ID_Unidad Temperatura,
    ID_Data Costo Unitario,
    Comentario Toma De Temperatura,
);
CREATE TABLE Data Animales Señas Particulares (
    ID_Data Seña Particular PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    Referencia Seña,
    Seña Particular,
);
CREATE TABLE Data Animales Servicios (
    ID_Data Servicios PRIMARY KEY,
    ID_Animal,
    FechaHora Actividad,
    Status Servicio,
    Inicio De Servicio,
    Fin De Servicio,
    ID_Data Costo Unitario,
    Comentario Servicio,
);
CREATE TABLE Data Animales Status (
    ID_Data Status,
    ID_Animal PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    ID_Status Animal,
);
CREATE TABLE Data Animales Ultimo Evento Registrado (
    ID_Data Ultimo Evento Registrado,
    ID_Animal PRIMARY KEY,
    FechaHora Actividad,
);
CREATE TABLE Data Bancos Calificacion y Limite De Credito (
    ID_Data Calificacion y Limite De Credito PRIMARY KEY,
    ID_Persona,
    FechaHora Actividad,
    Calificacion De Credito,
    Monto Limite De Credito,
    ID_Moneda,
    Comentario Calificacion y Limite De Credito,
);
CREATE TABLE Data Bancos Cuentas Bancarias (
    ID_Data Cuenta Bancaria,
    Numero De Cuenta PRIMARY KEY,
    Tipo De Cuenta PRIMARY KEY,
    Nombre De Banco PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    ID_Moneda,
    CBU,
    CBU Alias,
    Comentario Cuenta Bancaria,
);
CREATE TABLE Data Bancos Link_Cuentas_Personas (
    ID_Data Cuenta _Persona,
    ID_Persona PRIMARY KEY,
    ID_Data Cuenta Bancaria PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    Titular De Cuenta,
    Cuenta Activa,
    Comentario Cuenta_Persona,
);
CREATE TABLE Data Caravanas Datos (
    ID_Data_Caravana Datos PRIMARY KEY,
    ID_Caravana,
    FechaHora Actividad,
    RENSPA,
    ID_Data Costo Unitario,
    ID_Status Caravana,
    Comentario Caravana Datos,
);
CREATE TABLE Data Caravanas Identificadores Secundarios (
    ID_Data Caravana Identificador Secundario,
    ID_Caravana PRIMARY KEY,
    FechaHora Actividad,
    Alfanumerico,
    Cantidad De PMC,
    Comentario Identificador Secundario,
);
CREATE TABLE Data Establecimientos Parametros Tratamientos_Geografia (
    ID_Data Establecimiento Parametros Tratamiento,
    ID_Establecimiento PRIMARY KEY,
    ID_Tratamiento_Geografia PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    Fecha Aplicacion Inicial,
    Comentario Data Establecimiento Parametros Tratamiento,
);
CREATE TABLE Data Monedas De Referencia (
    ID_Data Moneda De Referencia,
    ID_Moneda PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    Comentario Moneda De Referencia,
);
CREATE TABLE Data Monedas Por Defecto (
    ID_Data Moneda Por Defecto,
    ID_Pais PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    ID_Moneda,
    Comentario Moneda Por Defecto,
);
CREATE TABLE Data Monedas Tasas de Cambio (
    ID_Data Tasa De Cambio,
    ID_Data Moneda De Referencia PRIMARY KEY,
    ID_Moneda Transaccional PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    Tasa De Cambio,
    Comentario Tasa De Cambio,
);
CREATE TABLE Data Paises_Unidades (
    ID_Data  Paises_Unidades,
    ID_Pais PRIMARY KEY,
    ID_Unidad PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
);
CREATE TABLE Data Parametros Generales Otros (
    ID_Parametro PRIMARY KEY,
    FechaHora Actividad,
    Nombre Parametro,
    Valor Parametro,
    Comentario Parametro,
);
CREATE TABLE Data Personas Datos (
    ID_Data Personas Datos PRIMARY KEY,
    ID_Persona,
    FechaHora Actividad,
    Dueño,
    Direccion,
    Ciudad,
    Codigo Postal,
    Provincia,
    Pais,
    ID_Status General,
    Situacion IVA,
    Persona Campo Adicional 1,
    Persona Campo Adicional 2,
    Persona Campo Adicional 3,
    Persona Campo Adicional 4,
);
CREATE TABLE Data TM Costos Unitarios (
    ID_Data Costo Unitario PRIMARY KEY,
    ID_Data Transacccion Monetaria,
    FechaHora Actividad,
    Monto,
    ID_Moneda,
    ID_Data Tasa De Cambio,
    Comentario Costo Unitario,
);
CREATE TABLE Data TM Precios Unitarios (
    ID_Data Precio Unitario PRIMARY KEY,
    ID_Data Transacccion Monetaria,
    FechaHora Actividad,
    Monto,
    ID_Moneda,
    ID_Data Tasa De Cambio,
    Comentario Precio Unitario,
);
CREATE TABLE Data TM Transacciones Monetarias (
    ID_Data Transaccion Monetaria PRIMARY KEY,
    FechaHora Actividad,
    Monto,
    Concepto,
    ID_Moneda,
    Tipo De Transaccion,
    ID_Data Tasa De Cambio,
    Comentario Transaccion Monetaria,
);
CREATE TABLE Data Vehiculos Datos (
    ID_Data Vehiculos Datos PRIMARY KEY,
    ID_Vehiculo,
    FechaHora Actividad,
    ID_Persona,
    ID_Status General,
    ID_Data Transaccion Monetaria,
    Dato Adicional Vehiculo 1,
    Dato Adicional Vehiculo 2,
    Dato Adicional Vehiculo 3,
    Comentario Datos Vehiculo,
);
CREATE TABLE Errores Codigos (
    ID_Codigo Error,
    Numero Codigo Error PRIMARY KEY,
    Descripcion Codigo Error,
    Mensaje De Error,
    Comentario Errores Codigos,
);
CREATE TABLE Explotacion Subtipos (
    ID_Explotacion Subtipo PRIMARY KEY,
    ID_Explotacion Tipo,
    SubTipo De Explotacion,
);
CREATE TABLE Explotacion Tipos (
    ID_ Explotacion Tipo,
    Tipo De Explotacion PRIMARY KEY,
    Comentario Explotacion,
);
CREATE TABLE Geo Departamentos (
    ID_Departamento,
    ID_Provincia PRIMARY KEY,
    Nombre Departamento PRIMARY KEY,
    Cabecera Departamento,
    ID_Nivel De Localizacion,
    Comentario Departamento,
);
CREATE TABLE Geo Establecimientos (
    ID_Establecimiento PRIMARY KEY,
    Nombre Establecimiento,
    Coordenadas Establecimiento Latitud,
    Coordenadas Establecimiento Longitud,
    Superfice,
    ID_Unidad,
    ID_Explotacion Tipo,
    ID_Nivel De Localizacion,
    Comentario Establecimiento,
);
CREATE TABLE Geo Establecimientos Locaciones (
    ID_Locacion,
    Nombre Locacion,
    ID_Lote,
    Parametro Locacion 1,
    Parametro Locacion 2,
    Parametro Locacion 3,
    Coordenadas Locacion Latitud,
    Coordenadas Locacion Longitud,
    ID_Nivel De Localizacion,
    Comentario Locacion,
);
CREATE TABLE Geo Establecimientos Lotes (
    ID_Lote PRIMARY KEY,
    Nombre Lote,
    ID_Establecimiento,
    ID_Provincia,
    ID_Explotacion Subtipo,
    Superficie Lote,
    ID_Unidad,
    Coordenadas Lote Latitud,
    Coordenadas Lote Longitud,
    ID_Nivel De Localizacion,
    Comentario Lote,
);
CREATE TABLE Geo Establecimientos Potreros (
    ID_Potrero,
    Nombre Potrero PRIMARY KEY,
    ID_Lote PRIMARY KEY,
    ID_Departamento PRIMARY KEY,
    Coordenadas Potrero Latitud,
    Coordenadas Potrero Longitud,
    Superficie Potrero,
    ID_Unidad,
    ID_Nivel De Localizacion,
    Comentario Potrero,
);
CREATE TABLE Geo Link Establecimientos_Provincias (
    ID_Link Establecimientos_Provincias,
    ID_Establecimiento PRIMARY KEY,
    ID_Provincia PRIMARY KEY,
    ID_Nivel De Localizacion,
    Comentario Establecimiento_Provincia,
);
CREATE TABLE Geo Link Lotes_Departamentos (
    ID_Link Lote_Departamento,
    ID_Lote PRIMARY KEY,
    ID_Departamento PRIMARY KEY,
    ID_Nivel De Localizacion,
    Comentario Lote_Departamento,
);
CREATE TABLE Geo Niveles De Localizacion (
    ID_Nivel De Localizacion PRIMARY KEY,
    Nombre Localizacion,
    Orden de Localizacion,
    Nombre Tabla Geo Asociada,
    Localizacion Activa,
    Comentario Nivel De Localizacion,
);
CREATE TABLE Geo Paises (
    ID_Pais,
    Nombre Pais PRIMARY KEY,
    Abreviacion Pais,
    ID_Nivel de Localizacion,
    Capital Pais,
);
CREATE TABLE Geo Provincias (
    ID_Provincia,
    ID_Pais PRIMARY KEY,
    Nombre Provincia PRIMARY KEY,
    Abreviacion,
    ID_Nivel De Localizacion,
    Capital Provincia,
);
CREATE TABLE Geo Region Pais (
    ID_Region Pais,
    ID_Pais PRIMARY KEY,
    Nombre Region Pais PRIMARY KEY,
    Abreviacion,
    ID_Nivel De Localizacion,
    Comentario Region Pais,
);
CREATE TABLE Geo Region Provincia (
    ID_Region Provincia,
    ID_Provincia PRIMARY KEY,
    Nombre Region Provincia PRIMARY KEY,
    Abreviacion,
    ID_Nivel De Localizacion,
    Comentario Region Provincia,
);
CREATE TABLE Link Actividades Programdas_Geografia PLANTILLA (
    ID_Actividad Programada_Geografia PRIMARY KEY,
    ID_Nombre Actividad Programada,
    ID_Nivel De Localizacion Requerido,
    FechaHora Actividad,
    ID_Provincia,
    ID_Departamento,
    ID_Establecimiento,
    ID_Lote,
    Fecha De Inicio Actividad,
    Recurrencia (Dias),
    Programada para M/H,
    Programada para Categorias,
    Comentario Actividad Programada_Geografia,
);
CREATE TABLE Link Personas_Contacto Telefonos (
    ID_Link Persona_Telefonos,
    ID_Telefono PRIMARY KEY,
    ID_Persona PRIMARY KEY,
    Comentario Persona_Telefono,
);
CREATE TABLE Link Personas_Contacto WebRedes (
    ID_Link Persona_Contacto WebRedes,
    ID_Contacto WebRedes PRIMARY KEY,
    ID_Persona PRIMARY KEY,
    Comentario Link Persona_WebRedes,
);
CREATE TABLE Link Raza_Categoria (
    ID_Link Raza_Categoria,
    ID_Categoria PRIMARY KEY,
    ID_Raza_Tipo De Animal PRIMARY KEY,
    Dias Inicio Categoria,
    Dias Final Categoria,
);
CREATE TABLE Link SP Tratamientos_Animales PLANTILLA (
    ID_Tratamiento_Animal PLANTILLA PRIMARY KEY,
    ID_Animal,
    ID_Link Tratamiento_Animal,
    ID_Tratamiento_Geografia,
    FechaHora Actividad,
    ID_Data Localizacion,
    Tratamiento OnOff,
    ID_Requerimiento De Sanidad,
    Recurrencia (Dias),
    Fecha Aplicacion Inicial,
    ID_Data Actividad Programada,
    Dias Desde La Fecha Actividad,
    Tratamiento Disparado Por Sistema,
    Comentario Tratamiento Animal Individual,
);
CREATE TABLE Link SP Tratamientos_Geografia PLANTILLA (
    ID_Tratamiento_Geografia,
    ID_Link Tratamiento_Animal PRIMARY KEY,
    ID_Nivel De Localizacion PRIMARY KEY,
    FechaHora Actividad PRIMARY KEY,
    ID_Pais,
    ID_Provincia,
    ID_Departamento,
    ID_Establecimiento,
    ID_Lote,
    ID_Requerimiento De Sanidad,
    Recurrencia (Dias),
    ID_Nombre Actividad Programada,
    Tratamiento OnOff,
    Comentario Tratamiento_Geografia,
);
CREATE TABLE Link SP Tratamientos_Tipos de Animal (
    ID_Link Tratamiento_Animal,
    ID_Tipo De Animal PRIMARY KEY,
    ID_Nombre De Tratamiento PRIMARY KEY,
    ID_Tratamiento Subcategoria PRIMARY KEY,
    Comentario Tratamiento_Animal,
);
CREATE TABLE Monedas Lista (
    ID_Moneda PRIMARY KEY,
    Nombre Moneda,
    Abreviacion Moneda,
    Simbolo Moneda,
    Comentario Moneda,
);
CREATE TABLE Personas (
    ID_Persona PRIMARY KEY,
    Nombre o Razón Social,
    Apellidos,
    DNI,
    CUIT_CUIL,
    CUIG,
    RENSPA,
    Persona Campo Adicional 1,
    Persona Campo Adicional 2,
    Persona Campo Adicional 3,
    Comentario Persona,
);
CREATE TABLE Status General (
    ID_Status General,
    Status Operacion PRIMARY KEY,
    Descripcion Status General,
);
CREATE TABLE Unidades Lista (
    ID_Unidad PRIMARY KEY,
    Nombre Unidad,
    Tipo Unidad,
    Sigla,
    ID_Sistema De Unidades,
    Comentario Unidad,
);
CREATE TABLE Unidades Sistemas (
    ID_Sistema De Unidades,
    Nombre Sistema De Unidades PRIMARY KEY,
    Comentario Sistema De Unidades,
);
CREATE TABLE Usuarios (
    ID_Usuario PRIMARY KEY,
    ID_Persona,
    FechaHora Actividad,
    Login Usuario,
    Clave Usuario,
    ID_Nivel De Acceso,
    ID_Status Usuario,
    Comentario Usuario,
);
CREATE TABLE Usuarios Niveles De Acceso (
    ID_Nivel De Acceso,
    Nivel de Acceso PRIMARY KEY,
    Nombre Nivel,
    Comentario Nivel,
);
CREATE TABLE Usuarios Status (
    ID_Status Usuario,
    Status Usuario PRIMARY KEY,
    Comentario Status Usuario,
);
CREATE TABLE Vehiculos (
    ID_Vehiculo PRIMARY KEY,
    Placa Vehiculo,
    Tipo De Vehiculo,
    Marca Vehiculo,
    Modelo Vehiculo,
    Fecha Entrada En Servicio,
    Numero Unidad,
    Alias Unidad,
    Otro Dato Vehiculo 1,
    Otro Dato Vehiculo 2,
    Comentario Vehiculo,
);
