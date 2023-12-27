
        # TODO(cmt): ORIGINAL VALUES. Before implemention UID_Previous logic.
""" Animales, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS Trigger_Animales_INSERT AFTER INSERT ON Animales FOR EACH ROW
BEGIN
UPDATE Animales SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1) WHERE Animales.ROWID == NEW.ROWID AND NEW.Terminal_ID IS NULL;

UPDATE Animales SET UID_Objeto_Original = (SELECT DISTINCT UID_Objeto FROM (SELECT DISTINCT UID_Objeto, "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL))
WHERE
"FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT UID_Objeto, "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND
INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL))))
WHERE Animales.ROWID == NEW.ROWID;

END;

""" Animales Check_Duplicates, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS Trigger_Animales_Check_Duplicates AFTER INSERT ON Animales FOR EACH ROW
WHEN
(SELECT DISTINCT UID_Objeto FROM Animales, json_each(Animales.Identificadores)
WHERE Animales.ROWID != NEW.ROWID AND json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND
("Salida YN" == 0 OR "Salida YN" IS NULL))
BEGIN
UPDATE _sys_Terminals SET "Tables With Duplicates" = json_insert("Tables With Duplicates", '$[#]', "Animales"||'-'||NEW.ROWID||'-'||
(SELECT DISTINCT UID_Objeto_Original FROM Animales, json_each(Animales.Identificadores)
WHERE Animales.ROWID != NEW.ROWID AND json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND
("Salida YN" == 0 OR "Salida YN" IS NULL)))
WHERE Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1);
END;


""" Geo Check_Duplicates, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS Trigger_Geo_Check_Duplicates AFTER INSERT ON "Geo Entidades" FOR EACH ROW
WHEN
(SELECT (SELECT COUNT(*) FROM "Geo Entidades" WHERE Identificadores_str == NEW. Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL)) > 1)
BEGIN
UPDATE _sys_Terminals SET "Tables With Duplicates" = json_insert("Tables With Duplicates", '$[#]', "Geo"||'-'||NEW.ROWID)
WHERE Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1);
END;


""" Caravanas on INSERT """
CREATE TRIGGER IF NOT EXISTS Trigger_Caravanas_INSERT AFTER INSERT ON Caravanas FOR EACH ROW
BEGIN
UPDATE Caravanas SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1) WHERE ROWID == NEW.ROWID AND NEW.Terminal_ID IS NULL;

UPDATE Caravanas SET UID_Objeto_Original = (SELECT DISTINCT UID_Objeto FROM (SELECT DISTINCT UID_Objeto, "FechaHora Registro" FROM Caravanas
WHERE  Identificadores_str == NEW. Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL))
WHERE"FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT UID_Objeto, "FechaHora Registro" FROM Caravanas
WHERE  Identificadores_str == NEW. Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL))))
WHERE Caravanas.ROWID == NEW.ROWID;

END;


""" Caravanas Check_Duplicates, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS Trigger_Caravanas_Check_Duplicates AFTER INSERT ON Caravanas FOR EACH ROW
WHEN
(SELECT (SELECT COUNT(*) FROM Caravanas WHERE Identificadores_str == NEW. Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL)) > 1)
BEGIN
UPDATE _sys_Terminals SET "Tables With Duplicates" = json_insert("Tables With Duplicates", '$[#]', "Caravanas"||'-'||NEW.ROWID)
WHERE Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1);
END;



# TODO: AFTER IMPLEMENTATION OF UID_Previous.
# This seems to be the correct INSERT trigger for Animales. UPDATEs ONLY records created by the same Terminal
""" Animales, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS Trigger_Animales_INSERT AFTER INSERT ON Animales FOR EACH ROW
WHEN NEW.Terminal_ID IS NULL OR NEW.Terminal_ID == (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1)
BEGIN
UPDATE Animales SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1) WHERE Animales.ROWID == NEW.ROWID AND NEW.Terminal_ID IS NULL;

UPDATE Animales SET UID_Objeto_Original = (SELECT DISTINCT UID_Objeto FROM (SELECT DISTINCT UID_Objeto, "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL))
WHERE
"FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT UID_Objeto, "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND
INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL))))
WHERE Animales.ROWID == NEW.ROWID;

UPDATE Animales SET UID_Previous = (SELECT DISTINCT UID_Objeto_Original FROM Animales, json_each(Animales.Identificadores)
WHERE Animales.ROWID != NEW.ROWID AND json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND
("Salida YN" == 0 OR "Salida YN" IS NULL)
AND
"FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT UID_Objeto, "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND Animales.ROWID != NEW.ROWID AND
INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL))))
WHERE Animales.ROWID == NEW.ROWID;
END;



""" Caravanas, ON INSERT"""
                # TODO: NOT TESTED!! But it implements a proper way to pull the record with MIN("FechaHora Registro")
CREATE TRIGGER IF NOT EXISTS Trigger_Caravanas_INSERT AFTER INSERT ON Caravanas FOR EACH ROW
WHEN NEW.Terminal_ID IS NULL OR NEW.Terminal_ID == (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1)
BEGIN
UPDATE Caravanas SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1) WHERE ROWID == NEW.ROWID AND NEW.Terminal_ID IS NULL;

UPDATE Caravanas SET UID_Objeto_Original = (SELECT DISTINCT UID_Objeto FROM (SELECT DISTINCT UID_Objeto, "FechaHora Registro" FROM Caravanas
WHERE  Identificadores_str == NEW. Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL))
WHERE "FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT UID_Objeto, "FechaHora Registro" FROM Caravanas
WHERE  Identificadores_str == NEW. Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL))))
WHERE Caravanas.ROWID == NEW.ROWID;

UPDATE Caravanas SET UID_Previous = (SELECT DISTINCT UID_Objeto_Original FROM (SELECT DISTINCT UID_Objeto_Original, "FechaHora Registro" FROM Caravanas
WHERE  Identificadores_str == NEW. Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL) AND Caravanas.ROWID != NEW.ROWID)
WHERE "FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT UID_Objeto, "FechaHora Registro" FROM Caravanas
WHERE  Identificadores_str == NEW. Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL)  AND Caravanas.ROWID != NEW.ROWID)))
WHERE Caravanas.ROWID == NEW.ROWID;

END;



#                       TODO:  DO-IT-ALL TRIGGERS ON INSERT - 06-Dec-23

""" Animales, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS Trigger_Animales_INSERT AFTER INSERT ON Animales FOR EACH ROW
BEGIN
UPDATE Animales SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1) , _Duplication_Index = NEW.UID_Objeto
WHERE Animales.ROWID == NEW.ROWID AND _Duplication_Index IS NULL;
UPDATE Animales SET _Duplication_Index = (SELECT DISTINCT _Duplication_Index FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL))
WHERE Animales.ROWID == NEW.ROWID;
END;


""" Caravanas, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS Trigger_Caravanas_INSERT AFTER INSERT ON Caravanas FOR EACH ROW
BEGIN
UPDATE Caravanas SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1) , _Duplication_Index = NEW.UID_Objeto
WHERE Caravanas.ROWID == NEW.ROWID AND _Duplication_Index IS NULL;
UPDATE Caravanas SET _Duplication_Index = IFNULL((SELECT DISTINCT _Duplication_Index FROM Caravanas
WHERE  Identificadores_str == NEW. Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL)), NEW.UID_Objeto)
WHERE Caravanas.ROWID == NEW.ROWID;
END;



""" GEO, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS Trigger_Geo_INSERT AFTER INSERT ON "Geo Entidades" FOR EACH ROW
BEGIN
UPDATE "Geo Entidades" SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1) , _Duplication_Index = NEW.UID_Objeto
WHERE "Geo Entidades".ROWID == NEW.ROWID AND _Duplication_Index IS NULL;
UPDATE "Geo Entidades" SET _Duplication_Index = IFNULL((SELECT DISTINCT _Duplication_Index FROM "Geo Entidades"
WHERE  Identificadores_str == NEW. Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL)), NEW.UID_Objeto)
WHERE "Geo Entidades".ROWID == NEW.ROWID;
END;

                        #  TODO:  DO-IT-ALL TRIGGERS ON INSERT - 06-Dec-23, including parentherised UPDATE clause.

""" Animales, ON INSERT"""

CREATE TRIGGER IF NOT EXISTS Trigger_Animales_INSERT AFTER INSERT ON Animales FOR EACH ROW
BEGIN
UPDATE Animales SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1), _Duplication_Index = NEW.UID_Objeto
WHERE Animales.ROWID == NEW.ROWID AND _Duplication_Index IS NULL;

UPDATE Animales SET (_Duplication_Index) = ((SELECT DISTINCT _Duplication_Index FROM (SELECT DISTINCT _Duplication_Index, "FechaHora Registro"
FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL))
WHERE
"FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND  INSTR(NEW.Identificadores_str, json_each.value) > 0
AND Animales.ROWID != NEW.ROWID AND ("Salida YN" == 0 OR "Salida YN" IS NULL)))))
WHERE Animales.ROWID == NEW.ROWID
AND
(SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0
AND Animales.ROWID != NEW.ROWID AND ("Salida YN" == 0 OR "Salida YN" IS NULL))) < NEW."FechaHora Registro";
END;



# TODO(cmt):  DO-IT-ALL TRIGGERS ON INSERT - 11-Dec-23, including parentherised UPDATE clause. FINAL VERSION (I hope).
        #  This version writes MULTIPLE records on each INSERT:
        # 1) Scans all records that qualify as duplicate records (share the criteria for equal Idenfiers).
        # 2) Gets min(fldTimeStamp) among all those records (pre-existing or "old" and the "new" incoming record). The
        # values on that record will be overwritten to the rest of the duplicate records.
        # 3) Writes data to all records that comply with the following:
        #   a. Their ROWIDs qualify as duplicates (meet the criteria for Identifiers),
        #   b. They are not inactive records ("Salida YN" is 0 or NULL),
        #   c. Their "FechaHora Registro" > min("FechaHora Registro") just found. This prevents from overwriting the
        # record that holds that minimum value. Just a detail.

""" Animales, ON INSERT"""

CREATE TRIGGER IF NOT EXISTS Trigger_Animales_INSERT AFTER INSERT ON Animales FOR EACH ROW
BEGIN
UPDATE Animales SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1), _Duplication_Index = NEW.UID_Objeto
WHERE Animales.ROWID == NEW.ROWID AND _Duplication_Index IS NULL;

UPDATE Animales SET (_Duplication_Index) = ((SELECT DISTINCT _Duplication_Index FROM (SELECT DISTINCT _Duplication_Index, "FechaHora Registro"
FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL))
WHERE
"FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND  INSTR(NEW.Identificadores_str, json_each.value) > 0
AND ("Salida YN" == 0 OR "Salida YN" IS NULL)))))

WHERE
Animales.ROWID IN (SELECT "ID_Animal" FROM (SELECT DISTINCT "ID_Animal", "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0
AND ("Salida YN" == 0 OR "Salida YN" IS NULL)
AND "FechaHora Registro" > (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0
AND ("Salida YN" == 0 OR "Salida YN" IS NULL)))));
END;

f'WHERE
f'Animales.ROWID IN (SELECT "ID_Animal" FROM (SELECT DISTINCT "ID_Animal", "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
f'WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0
f'AND ("Salida YN" == 0 OR "Salida YN" IS NULL)
f'AND "FechaHora Registro" > (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
f'WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0
f'AND ("Salida YN" == 0 OR "Salida YN" IS NULL)))));
f'END;



# TODO(cmt):  DO-IT-ALL TRIGGERS ON INSERT - 13-Dec-23. ANOTHER FINAL VERSION (I hope).
        #  This version writes MULTIPLE records on each INSERT:
        # 1) Scans all records that qualify as duplicate records (share the criteria for equal Idenfiers).
        # 2) Gets min(fldTimeStamp) among all those records (pre-existing or "old" and the "new" incoming record). The
        # values on that record will be overwritten to the rest of the duplicate records.
        # 3) Writes data to all records that comply with the following:
        #   a. Their ROWIDs qualify as duplicates (meet the criteria for Identifiers),
        #   b. They are not inactive records ("Salida YN" is 0 or NULL),
        #   c. Their "FechaHora Registro" > min("FechaHora Registro") just found. This prevents from overwriting the
        # record that holds that minimum value. Just a detail.
        # TODO(cmt): This version includes updating _sys_Terminals using the table name from _sys_Trigger_Table (last UPDATE)

CREATE TRIGGER IF NOT EXISTS Trigger_Animales_INSERT AFTER INSERT ON Animales FOR EACH ROW
BEGIN
UPDATE Animales SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1), _Duplication_Index = NEW.UID_Objeto
WHERE Animales.ROWID == NEW.ROWID AND _Duplication_Index IS NULL;

UPDATE Animales SET (_Duplication_Index) = ((SELECT DISTINCT _Duplication_Index FROM (SELECT DISTINCT _Duplication_Index, "FechaHora Registro"
FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL))
WHERE
"FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND  INSTR(NEW.Identificadores_str, json_each.value) > 0
AND ("Salida YN" == 0 OR "Salida YN" IS NULL)))))

WHERE
Animales.ROWID IN (SELECT "ID_Animal" FROM (SELECT DISTINCT "ID_Animal", "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0
AND ("Salida YN" == 0 OR "Salida YN" IS NULL)
AND "FechaHora Registro" > (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores)
WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL)))));

UPDATE _sys_Terminals SET "Tables With Duplicates" = json_insert("Tables With Duplicates", '$[#]', (SELECT DB_Table_Name FROM _sys_Trigger_Tables WHERE DB_Table_Name == "Animales"))
WHERE NEW._Duplication_Index IS NOT NULL AND
Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1);

END;





# TODO(cmt): *************  DO-IT-ALL TRIGGERS ON INSERT - 16-Dec-23. 3rd FINAL VERSION, Not quite there yet. **********

""" Animales, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS "Trigger_Animales_INSERT" AFTER INSERT ON "Animales" FOR EACH ROW
BEGIN
UPDATE "Animales" SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1), _Duplication_Index = NEW.UID_Objeto WHERE "Animales".ROWID == NEW.ROWID AND _Duplication_Index IS NULL;
UPDATE "Animales" SET (_Duplication_Index, 'ID_Clase De Animal', 'ID_Raza', 'Fecha De Nacimiento', 'Comentario', 'UID_Objeto_Original', 'ID_Animal Madre', 'ID_Animal Padre', 'Tipo De Gestacion', 'ID_Madre Subrogante', 'Nombre', 'Record UPDATE', 'Identificadores', 'ID_Usuario', 'Castrado YN', 'Salida YN', 'Modo', 'MachoHembra', 'Bitmask', 'TimeStamp Sync') = (((SELECT DISTINCT _Duplication_Index FROM (SELECT DISTINCT _Duplication_Index, "FechaHora Registro" FROM "Animales", json_each("Animales".Identificadores) WHERE json_valid("Animales".Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL)) WHERE "FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM "Animales", json_each("Animales".Identificadores) WHERE json_valid("Animales".Identificadores) AND  INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL))))), NEW."ID_Clase De Animal", NEW."ID_Raza", NEW."Fecha De Nacimiento", NEW."Comentario", NEW."UID_Objeto_Original", NEW."ID_Animal Madre", NEW."ID_Animal Padre", NEW."Tipo De Gestacion", NEW."ID_Madre Subrogante", NEW."Nombre", NEW."Record UPDATE", NEW."Identificadores", NEW."ID_Usuario", NEW."Castrado YN", NEW."Salida YN", NEW."Modo", NEW."MachoHembra", NEW."Bitmask", NEW."TimeStamp Sync") WHERE "Animales".ROWID IN (SELECT "ID_Animal" FROM (SELECT DISTINCT "ID_Animal", "FechaHora Registro" FROM "Animales", json_each("Animales".Identificadores) WHERE json_valid("Animales".Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL) AND "FechaHora Registro" >= (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM "Animales", json_each("Animales".Identificadores) WHERE json_valid("Animales".Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL)))));
UPDATE _sys_Terminals SET "Tables With Duplicates" = json_insert("Tables With Duplicates", "$[#]", "Animales" ||"-"|| NEW."ID_Clase De Animal") WHERE NEW._Duplication_Index IS NOT NULL AND Terminal_ID == (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1);
END;




""" Caravanas, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS "Trigger_Caravanas_INSERT" AFTER INSERT ON "Caravanas" FOR EACH ROW BEGIN

UPDATE "Caravanas" SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1), _Duplication_Index = NEW.UID_Objeto WHERE "Caravanas".ROWID == NEW.ROWID AND _Duplication_Index IS NULL;

UPDATE "Caravanas" SET (_Duplication_Index, 'ID_Color', 'Numero Caravana', 'ID_Tecnologia De Caravana', 'ID_Tipo De Caravana', 'ID_Formato De Caravana', 'Comentario', 'Asignada A Clase', 'Record UPDATE', 'Cantidad De Marcadores', 'Imagen', 'ID_Usuario', 'Salida YN', 'ID_Item', 'Asignada a UID', 'PushUpload', 'Bitmask', 'TimeStamp Sync') = ((SELECT DISTINCT _Duplication_Index FROM Caravanas WHERE Identificadores_str == NEW. Identificadores_str AND "FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM Caravanas WHERE Identificadores_str == NEW.Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL))) AND ("Salida YN" == 0 OR "Salida YN" IS NULL)), NEW."ID_Color", NEW."Numero Caravana", NEW."ID_Tecnologia De Caravana", NEW."ID_Tipo De Caravana", NEW."ID_Formato De Caravana", NEW."Comentario", NEW."Asignada A Clase", NEW."Record UPDATE", NEW."Cantidad De Marcadores", NEW."Imagen", NEW."ID_Usuario", NEW."Salida YN", NEW."ID_Item", NEW."Asignada a UID", NEW."PushUpload", NEW."Bitmask", NEW."TimeStamp Sync") WHERE Caravanas.ROWID IN (SELECT "ID_Caravana" FROM (SELECT DISTINCT "ID_Caravana", "FechaHora Registro" FROM Caravanas WHERE Identificadores_str == NEW.Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL) AND "FechaHora Registro" >= (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM Caravanas WHERE Identificadores_str == NEW.Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL)))));

UPDATE _sys_Terminals SET "Tables With Duplicates" = json_insert("Tables With Duplicates", "$[#]", "Caravanas") WHERE NEW._Duplication_Index IS NOT NULL AND Terminal_ID == (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1);
END;




""" Geo, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS "Trigger_Geo Entidades_INSERT" AFTER INSERT ON "Geo Entidades" FOR EACH ROW BEGIN
UPDATE "Geo Entidades" SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1), _Duplication_Index = NEW.UID_Objeto WHERE "Geo Entidades".ROWID == NEW.ROWID AND _Duplication_Index IS NULL;
UPDATE "Geo Entidades" SET (_Duplication_Index, 'Salida YN', 'ID_Nivel De Localizacion', 'ID_Unidad', 'Abreviacion Nombre', 'Entidad Estado YN', 'Superficie', 'Comentario', 'Containers', 'Record UPDATE', 'ID_Usuario', 'Entidad Activa YN', 'Nombre Entidad', 'ID_Tipo De Entidad', 'Bitmask', 'TimeStamp Sync') = ((SELECT DISTINCT _Duplication_Index FROM "Geo Entidades" WHERE Identificadores_str == NEW. Identificadores_str AND "FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM "Geo Entidades" WHERE Identificadores_str == NEW.Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL))) AND ("Salida YN" == 0 OR "Salida YN" IS NULL)), NEW."Salida YN", NEW."ID_Nivel De Localizacion", NEW."ID_Unidad", NEW."Abreviacion Nombre", NEW."Entidad Estado YN", NEW."Superficie", NEW."Comentario", NEW."Containers", NEW."Record UPDATE", NEW."ID_Usuario", NEW."Entidad Activa YN", NEW."Nombre Entidad", NEW."ID_Tipo De Entidad", NEW."Bitmask", NEW."TimeStamp Sync") WHERE "Geo Entidades".ROWID IN (SELECT "ID_GeoEntidad" FROM (SELECT DISTINCT "ID_GeoEntidad", "FechaHora Registro" FROM "Geo Entidades" WHERE Identificadores_str == NEW.Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL) AND "FechaHora Registro" >= (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM "Geo Entidades" WHERE Identificadores_str == NEW.Identificadores_str AND ("Salida YN" == 0 OR "Salida YN" IS NULL)))));
UPDATE _sys_Terminals SET "Tables With Duplicates" = json_insert("Tables With Duplicates", "$[#]", "Geo Entidades") WHERE NEW._Duplication_Index IS NOT NULL AND Terminal_ID == (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1);
END;



# TODO(cmt): *************  DO-IT-ALL TRIGGERS ON INSERT - 17-Dec-23. 4th FINAL VERSION, HOPE THIS IS IT! **********
        #  This one adds discrimination logic in the UPDATE of _sys_Terminals to avoid inserting duplicate values in the
        # JSON array. This is important not for Animales, Caravanas, etc but for Registro De Actividades. It must be
        # implemented there

""" Animales, ON INSERT"""
CREATE TRIGGER IF NOT EXISTS "Trigger_Animales_INSERT" AFTER INSERT ON "Animales" FOR EACH ROW
BEGIN
UPDATE "Animales" SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1), _Duplication_Index = NEW.UID_Objeto WHERE "Animales".ROWID == NEW.ROWID AND _Duplication_Index IS NULL;

UPDATE "Animales" SET (_Duplication_Index, 'ID_Clase De Animal', 'ID_Raza', 'Fecha De Nacimiento', 'Comentario', 'UID_Objeto_Original', 'ID_Animal Madre', 'ID_Animal Padre', 'Tipo De Gestacion', 'ID_Madre Subrogante', 'Nombre', 'Record UPDATE', 'Identificadores', 'ID_Usuario', 'Castrado YN', 'Salida YN', 'Modo', 'MachoHembra', 'Bitmask', 'TimeStamp Sync') = (((SELECT DISTINCT _Duplication_Index FROM (SELECT DISTINCT _Duplication_Index, "FechaHora Registro" FROM "Animales", json_each("Animales".Identificadores) WHERE json_valid("Animales".Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL)) WHERE "FechaHora Registro" == (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM "Animales", json_each("Animales".Identificadores) WHERE json_valid("Animales".Identificadores) AND  INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL))))), NEW."ID_Clase De Animal", NEW."ID_Raza", NEW."Fecha De Nacimiento", NEW."Comentario", NEW."UID_Objeto_Original", NEW."ID_Animal Madre", NEW."ID_Animal Padre", NEW."Tipo De Gestacion", NEW."ID_Madre Subrogante", NEW."Nombre", NEW."Record UPDATE", NEW."Identificadores", NEW."ID_Usuario", NEW."Castrado YN", NEW."Salida YN", NEW."Modo", NEW."MachoHembra", NEW."Bitmask", NEW."TimeStamp Sync") WHERE Animales.ROWID IN (SELECT "ID_Animal" FROM (SELECT DISTINCT "ID_Animal", "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores) WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL) AND "FechaHora Registro" >= (SELECT MIN("FechaHora Registro") FROM (SELECT DISTINCT "FechaHora Registro" FROM Animales, json_each(Animales.Identificadores) WHERE json_valid(Animales.Identificadores) AND INSTR(NEW.Identificadores_str, json_each.value) > 0 AND ("Salida YN" == 0 OR "Salida YN" IS NULL)))));

UPDATE _sys_Terminals SET "Tables With Duplicates" = json_set("Tables With Duplicates", '$[#]', "Animales" ||"-"|| NEW."ID_Clase De Animal") WHERE NEW._Duplication_Index IS NOT NULL AND Terminal_ID == (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1)

AND (SELECT _sys_Terminals."Tables With Duplicates" FROM _sys_Terminals, json_each(_sys_Terminals."Tables With Duplicates") WHERE Terminal_ID == "c1434644fb73439e8006acf03ca25df4"
AND json_valid(_sys_Terminals."Tables With Duplicates") AND INSTR(CAST(json_each.json AS TEXT),  "Animales" ||"-"|| NEW."ID_Clase De Animal" ) == 0 ) > 0;

END;


""" Animales Registro De Actividades, ON INSERT"""

CREATE TRIGGER IF NOT EXISTS "Trigger_Animales Registro De Actividades_INSERT" AFTER INSERT ON "Animales Registro De Actividades" FOR EACH ROW BEGIN
UPDATE "Animales Registro De Actividades" SET Terminal_ID = (SELECT Terminal_ID FROM _sys_terminal_id LIMIT 1) WHERE ROWID=new.ROWID AND NEW.Terminal_ID IS NULL;
UPDATE _sys_Trigger_Tables SET TimeStamp = NEW."FechaHora Registro" WHERE DB_Table_Name == "Animales Registro De Actividades" AND _sys_Trigger_Tables.ROWID == _sys_Trigger_Tables.Flag_ROWID AND NEW.Terminal_ID IS NOT NULL;

UPDATE _sys_Trigger_Tables SET "Updated_By" = json_set("Updated_By", '$[#]', NEW."ID_Clase De Animal") WHERE DB_Table_Name == "Animales Registro De Actividades" AND _sys_Trigger_Tables.ROWID == _sys_Trigger_Tables.Flag_ROWID AND NEW.Terminal_ID IS NOT NULL
AND (SELECT _sys_Trigger_Tables."Updated_By" FROM _sys_Trigger_Tables, json_each(_sys_Trigger_Tables."Updated_By") WHERE _sys_Trigger_Tables.ROWID == _sys_Trigger_Tables.Flag_ROWID
AND json_valid(_sys_Trigger_Tables."Updated_By") AND INSTR(CAST(json_each.json AS TEXT), NEW."ID_Clase De Animal") == 0 ) > 0;

END;



# TODO(cmt): ************ DO-IT-ALL TRIGGERS ON INSERT 19-Dec-23. 5th FINAL VERSION, NOW REALLY HOPE THIS IS IT! *********
# TODO(cmt): Unifies the use of _sys_Trigger_Tables for handling trigger updates. Deprecates _sys_Terminals table.
# Only needs to fix the UPDATE Statement for the _sys_Terminals tables in triggers for Animales, Caravanas and Geo.

""" Animales, ON INSERT"""



""" Caravanas, ON INSERT"""


""" Geo, ON INSERT"""



# TODO(cmt): On the one below, run a WHEN clause to execute trigger ONLY for records coming from other nodes and reduce
# some execution time. TODO: This requires that Terminal_ID in tables Registro De Activitdades by set in python.
""" Animales Registro De Actividades, ON INSERT"""
