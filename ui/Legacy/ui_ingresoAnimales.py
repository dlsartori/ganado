#!/usr/bin/env python
# -*- coding: utf-8 -*-
from PyQt5.QtCore import Qt, QDate
from PyQt5.QtSql import QSqlDatabase, QSqlTableModel, QSqlQuery, QSqlQueryModel
from PyQt5.QtWidgets import QApplication, QMessageBox, QPushButton
from PyQt5 import uic
import datetime
import sys
# https://stackoverflow.com/questions/32914488/global-variables-between-different-modules
# import cfg  # bind objects referenced by variables in cfg to like-named variables in this module
# These objects from 'cfg' now have an additional reference here
#
from comboTabla import ComboTabla
from ui_ingresoAnimalesAdic import DialogoAnimalesAdic
from ui_ingresoAnimalesCaravanas import DialogoAnimalesCaravanas
# https://www.riverbankcomputing.com/static/Docs/PyQt5/designer.html#PyQt5.uic.loadUiType
# https://stackoverflow.com/questions/22663716/working-with-pyqt-and-qt-designer-ui-files
DialogUI, DialogBase = uic.loadUiType("IngresoAnimales.ui")


class DialogoInsertarAnimales(DialogBase, DialogUI):
    """
    Dialog for insertion of new animals

    On accept, write to database
    """
    def __init__(self, intTipoAnimal=1, parent=None):      # Por ahora, intTipoAnimal hardcoded en "Vacunos"
        DialogBase.__init__(self, parent)
        self.setupUi(self)

        # [self.db, self.dbGanadoConnCreated] = createDbConn("GanadoSQLite.db", "GanadoConnection")
        # print(tbl('tblAnimales'))
        # print(fld('tblAnimales', 'fldID'))

        # Set attributes
        self.intTipoAnimal = intTipoAnimal
        self.lastKey = -1
        # from additional dialogs
        self.qdtFechaNac = QDate.currentDate().addDays(-80)    # as QDate - por defecto, Edad = 80 días
        self.qdtFechaDestete = None    # as QDate
        self.qdtFechaServ = None      # as QDate
        self.dblPeso = None
        self.intLote = None
        self.intPotrero = None
        self.intLocacion = None
        self.blnTratamientos = None
        self.blnDieta = None
        self.lstIdCaravanas = []

        # # Populate Combo Boxes
        # modelTipo = QSqlQueryModel()
        # strSQL = 'SELECT [' + fld('tblAnimalesTiposDeAlta/Baja', 'fldID') + '], [' + \
        #         fld('tblAnimalesTiposDeAlta/Baja', 'fldName') + \
        #         '] FROM [' + tbl('tblAnimalesTiposDeAlta/Baja') + '] ' + \
        #         ' WHERE [Comentario Tipo De Alta/Baja] LIKE "Alta%" '
        # # ' ORDER BY [' + fld('tblAnimalesEntradaTipos', 'fldEntryType') + ']'
        # modelTipo.setQuery(QSqlQuery(strSQL, self.db))
        # modelCategoria = QSqlQueryModel()
        # strSQL = 'SELECT [' + fld('tblAnimalesCategorias', 'fldID') + '], [' + \
        #          fld('tblAnimalesCategorias', 'fldName') + \
        #          '] FROM [' + tbl('tblAnimalesCategorias') + '] ' + \
        #          ' WHERE [' + fld('tblAnimalesCategorias', 'fldFK_TipoDeAnimal') + '] = ' + str(self.intTipoAnimal) + \
        #          ' ORDER BY [' + fld('tblAnimalesCategorias', 'fldName') + ']'
        # modelCategoria.setQuery(QSqlQuery(strSQL, self.db))
        # modelRaza = QSqlQueryModel()
        # strSQL = 'SELECT [' + fld('tblAnimalesRazas', 'fldID') + '], [' + \
        #          fld('tblAnimalesRazas', 'fldName') + \
        #          '] FROM [' + tbl('tblAnimalesRazas') + '] ' + \
        #          ' ORDER BY [' + fld('tblAnimalesRazas', 'fldName') + ']'
        # modelRaza.setQuery(QSqlQuery(strSQL, self.db))
        # modelEstablecimiento = QSqlQueryModel()
        # strSQL = 'SELECT [' + fld('tblGeoEstablecimientos', 'fldID') + '], [' + \
        #          fld('tblGeoEstablecimientos', 'fldName') + \
        #          '] FROM [' + tbl('tblGeoEstablecimientos') + '] ' + \
        #          ' ORDER BY [' + fld('tblGeoEstablecimientos', 'fldName') + ']'
        # modelEstablecimiento.setQuery(QSqlQuery(strSQL, self.db))
        # modelDuenio = QSqlQueryModel()
        # strSQL = 'SELECT [' + fld('tblPersonas', 'fldID') + '], [' + \
        #          fld('tblPersonas', 'fldName') + \
        #          '] FROM [' + tbl('tblPersonas') + '] ' + \
        #          ' ORDER BY [' + fld('tblPersonas', 'fldName') + ']'
        # modelDuenio.setQuery(QSqlQuery(strSQL, self.db))
        # modelMarca = QSqlQueryModel()
        # strSQL = 'SELECT [' + fld('tblAnimalesMarcas', 'fldID') + '], [' + \
        #          fld('tblAnimalesMarcas', 'fldName') + \
        #          '] FROM [' + tbl('tblAnimalesMarcas') + '] ' + \
        #          ' ORDER BY [' + fld('tblAnimalesMarcas', 'fldName') + ']'
        # modelMarca.setQuery(QSqlQuery(strSQL, self.db))
        #
        # self.cboTipoDeEntrada.setModel(modelTipo)
        # self.cboTipoDeEntrada.setModelColumn(1)
        # self.cboCategoria.setModel(modelCategoria)
        # self.cboCategoria.setModelColumn(1)
        # self.cboRaza.setModel(modelRaza)
        # self.cboRaza.setModelColumn(1)
        # self.cboEstablecimiento.setModel(modelEstablecimiento)
        # self.cboEstablecimiento.setModelColumn(1)
        # self.cboDuenio.setModel(modelDuenio)
        # self.cboDuenio.setModelColumn(1)
        # self.cboMarca.setModel(modelMarca)
        # self.cboMarca.setModelColumn(1)
        # # Select stock entry as default
        # intPosTipoEntrada = self.cboTipoDeEntrada.findData('Ingreso En Stock', Qt.DisplayRole)
        # self.cboTipoDeEntrada.setCurrentIndex(intPosTipoEntrada)

        # Signals - Slots
        # self.btnOk.clicked.connect(self.commit)
        # self.btnCancel.clicked.connect(self.close)
        self.btnAdic.clicked.connect(self.dialogDatosAdicionales)
        self.btnCaravanas.clicked.connect(self.dialogSelCaravanas)
        # self.chkCastr.toggled.connect(self.onChkCastrPregnToggle)
        # self.chkPreniez.toggled.connect(self.onChkCastrPregnToggle)

    def onChkCastrPregnToggle(self):
        """
        manage pregnant/castrated checkboxes visibility

        :return: None
        """
        if self.chkCastr.isChecked():
            self.chkPreniez.setEnabled(False)
        else:
            self.chkPreniez.setEnabled(True)
        if self.chkPreniez.isChecked():
            self.chkCastr.setEnabled(False)
        else:
            self.chkCastr.setEnabled(True)

    def dialogDatosAdicionales(self):
        """
        Invokes the additional animal data selection dialog

        On dialog accept, update attributes

        :return: None
        """
        dialog = DialogoAnimalesAdic(intLote=self.intLote, intPotrero=self.intPotrero, intLocacion=self.intLocacion,
                                     dblPeso=self.dblPeso, qdtFechaNac=self.qdtFechaNac,
                                     qdtFechaDestete=self.qdtFechaDestete, qdtFechaServ=self.qdtFechaServ,
                                     blnTratamientos=self.blnTratamientos, blnDieta=self.blnDieta)

        if dialog.exec_():
            # self.qdtFechaNac = dialog.qdtFechaNac
            # self.qdtFechaDestete = dialog.qdtFechaDestete
            # self.qdtFechaServ = dialog.qdtFechaServ
            # self.intLote = dialog.intLote
            # self.intPotrero = dialog.intPotrero
            # self.intLocacion = dialog.intLocacion
            # self.dblPeso = dialog.dblPeso
            # self.blnTratamientos = dialog.blnTratamientos
            # self.blnDieta = dialog.blnDieta
            del dialog

    def dialogSelCaravanas(self):
        """
        Invokes the tag selection dialog

        On dialog accept, update lstIdCaravanas attribute

        :return: None
        """
        dialogCaravanas = DialogoAnimalesCaravanas(self.lstIdCaravanas)
        dialogCaravanas.exec_()

        # if dialogCaravanas.accepted:
        #     self.lstIdCaravanas = []
        #     if len(dialogCaravanas.lstIDs) > 0:
        #         for item in dialogCaravanas.lstIDs:
        #             self.lstIdCaravanas.append(item)

        del dialogCaravanas

    # def commit(self):
    #     # 1) Validation
    #     #    - Combo Boxes: select a value from the list
    #     for wdg in self.children():
    #         if wdg.__class__.__name__ == 'ComboTabla':
    #             if wdg.findText(wdg.currentText()) == -1:
    #                 QMessageBox.warning(self, "Valor",
    #                                     "Se debe elegir un valor de la lista")
    #                 wdg.setFocus()
    #                 return
    #     #    - Not null values
    #     #    - Duplicate values
    #
    #     # 2) Data collection: define variables whith values for each field to be written
    #     #    - direct: taken from form widgets
    #     #       - add animal dialog
    #     intTipoEntrada = self.cboTipoDeEntrada.model().index(self.cboTipoDeEntrada.currentIndex(),0).data()
    #     intCategoria = self.cboCategoria.model().index(self.cboCategoria.currentIndex(),0).data()
    #     blnCastr = True if self.chkCastr.isChecked() else False
    #     blnPregn = True if self.chkPreniez.isChecked() else False
    #     intRaza = self.cboRaza.model().index(self.cboRaza.currentIndex(),0).data()
    #     intEstablecimiento = self.cboEstablecimiento.model().index(self.cboEstablecimiento.currentIndex(),0).data()
    #     intDuenio = self.cboDuenio.model().index(self.cboDuenio.currentIndex(),0).data()
    #     intMarca = self.cboMarca.model().index(self.cboMarca.currentIndex(),0).data()
    #     strComentario = self.pteComentario.toPlainText()
    #     #       - tag select dialog → values set in self.dialogSelCaravanas
    #     #       - additional animal data dialog → values set in self.dialogDatosAdicionales
    #     #    - calculados
    #     dateTime = datetime.datetime.now()
    #
    #     # 3) Input:
    #     # TEST
    #     strResult = f'Tabla {tblBr("tblAnimales")}:' \
    #                 f'\n    intRaza: {intRaza} → {fldBr("tblAnimales","fldFK_Raza").split(".")[1]}' \
    #                 f'\n    self.qdtFechaNac: {self.qdtFechaNac} → {fldBr("tblAnimales","fldDOB").split(".")[1]}' \
    #                 f'\n    self.qdtFechaServ: {self.qdtFechaServ} → {fldBr("tblAnimales","fldDate").split(".")[1]}' \
    #                 f'\n    strComentario: {strComentario} → {fldBr("tblAnimales","fldComment").split(".")[1]}' \
    #                 f'\nTabla {tblBr("tblDataAnimalesCategorias")}:' \
    #                 f'\n    intCategoria: {intCategoria} → {fldBr("tblDataAnimalesCategorias","fldFK_Categoria").split(".")[1]}' \
    #                 f'\nTabla {tblBr("tblDataAnimalesActividadAlta")}:' \
    #                 f'\n    intTipoEntrada: {intTipoEntrada} → {fldBr("tblDataAnimalesActividadAlta","fldFK_TipoDeAlta/Baja").split(".")[1]}' \
    #                 f'\nTabla {tblBr("tblCaravanasRegistroDeActividades")}:' \
    #                 f'\n    self.lstIdCaravanas: {self.lstIdCaravanas} → {fldBr("tblCaravanasRegistroDeActividades","fldFK_Caravana").split(".")[1]} ' \
    #                 f'\n    (un registro por cada caravana)' \
    #                 f'\nTabla {tblBr("tblDataAnimalesActividadCastracion")}:' \
    #                 f'\n    blnCastr: {blnCastr} → {fldBr("tblDataAnimalesActividadCastracion","fldCastrado").split(".")[1]}' \
    #                 f'\nTabla {tblBr("tblDataAnimalesActividadPreñez")}:' \
    #                 f'\n    blnPregn: {blnPregn} → {fldBr("tblDataAnimalesActividadPreñez","fldPregnant").split(".")[1]}' \
    #                 f'\nTabla {tblBr("tblDataAnimalesActividadPesaje")}:' \
    #                 f'\n    self.dblPeso: {self.dblPeso} → {fldBr("tblDataAnimalesActividadPesaje","fldWeight").split(".")[1]}' \
    #                 f'\nTabla {tblBr("tblDataAnimalesActividadDestete")}:' \
    #                 f'\n    self.qdtFechaDestete: {self.qdtFechaDestete} → {fldBr("tblDataAnimalesActividadDestete","fldTimeStamp").split(".")[1]}' \
    #                 f'\nTabla {tblBr("tblDataAnimalesActividadMarca")}:' \
    #                 f'\n    intMarca: {intMarca} → {fldBr("tblDataAnimalesActividadMarca","fldFK_Marca").split(".")[1]}' \
    #                 f'\nTabla {tblBr("tblDataAnimalesActividadAsignacionDeDueño")}:' \
    #                 f'\n    intDuenio: {intDuenio} → {fldBr("tblDataAnimalesActividadAsignacionDeDueño","fldFK_PersonaDueño").split(".")[1]}' \
    #                 f'\nTabla {tblBr("tblGeoRegistroDeLocalizacion")}:' \
    #                 f'\n    intEstablecimiento: {intEstablecimiento} → {fldBr("tblGeoRegistroDeLocalizacion","fldFK_LinkEstablecimientos_Provincias").split(".")[1]}' \
    #                 f'\n    self.intLote: {self.intLote} → {fldBr("tblGeoRegistroDeLocalizacion","fldFK_Lote").split(".")[1]}' \
    #                 f'\n    self.intPotrero: {self.intPotrero} → {fldBr("tblGeoRegistroDeLocalizacion","fldFK_Potrero").split(".")[1]}' \
    #                 f'\n    self.intLocacion: {self.intLocacion} → {fldBr("tblGeoRegistroDeLocalizacion","fldFK_Locacion").split(".")[1]}' \
    #                 f'\n\nSin definir aún:' \
    #                 f'\n    self.blnTratamientos: {self.blnTratamientos} → ????' \
    #                 f'\n    self.blnDieta: {self.blnDieta} → ????' \
    #                 f'\n\ndateTime: {dateTime} → Campos [FechaHora Actividad]'
    #     # QMessageBox.information(self, "Datos a ingresar (obtenidos de los diálogos)", strResult)
    #
    #     # Generate and run insertion queries for each table
    #     query = QSqlQuery(self.db)
    #     # [Animales] table
    #     intIdAnimal = maxId('tblAnimales') + 1
    #     intTipoAnimal = 1
    #     strTimeStamp = dateTime.date().isoformat() + ' ' + dateTime.time().isoformat()
    #     # T O  A D D : if date of birth is NULL, take it from animal category
    #     strFechaNac = "\'" + self.qdtFechaNac.toPyDate().isoformat() + "\'" if self.qdtFechaNac else "null"
    #     strFechaServ = "\'" + self.qdtFechaServ.toPyDate().isoformat() + "\'" if self.qdtFechaServ else "null"
    #     strSQLAnimales = f"" \
    #         f"INSERT INTO {tblBr('tblAnimales')} (" \
    #         f" [{fld('tblAnimales', 'fldID')}]," \
    #         f" [{fld('tblAnimales', 'fldFK_TipoDeAnimal')}]," \
    #         f" [{fld('tblAnimales', 'fldFK_Raza')}]," \
    #         f" [{fld('tblAnimales', 'fldTimeStamp')}]," \
    #         f" [{fld('tblAnimales', 'fldDOB')}]," \
    #         f" [{fld('tblAnimales', 'fldDate')}]," \
    #         f" [{fld('tblAnimales', 'fldComment')}]" \
    #         f" ) VALUES ({intIdAnimal}, {intTipoAnimal}, {intCategoria}, '{strTimeStamp}'," \
    #         f" {strFechaNac}, {strFechaServ}, '{strComentario}' " \
    #         f" );"
    #     print(strSQLAnimales)
    #     # [Animales Registro De Actividades] table
    #     # Hardcoded values:
    #     #   [ID_Nombre Actividad] → 1 (Alta De Animal)
    #     #   [Inventario] → 1 (es actividad de inventario)
    #     #   [Flag Operation Por Lote] → 0 (no es operación por lote)
    #     intIdAnimalesRegistroActividades = maxId('tblAnimalesRegistroDeActividades') + 1
    #     strSQLAnimalesRegistroActividades = f"" \
    #         f"INSERT INTO {tblBr('tblAnimalesRegistroDeActividades')} (" \
    #         f" [{fld('tblAnimalesRegistroDeActividades', 'fldID')}]," \
    #         f" [{fld('tblAnimalesRegistroDeActividades', 'fldFK_Animal')}]," \
    #         f" [{fld('tblAnimalesRegistroDeActividades', 'fldFK_NombreActividad')}]," \
    #         f" [{fld('tblAnimalesRegistroDeActividades', 'fldTimeStamp')}]," \
    #         f" [{fld('tblAnimalesRegistroDeActividades', 'fldInventory')}]," \
    #         f" [{fld('tblAnimalesRegistroDeActividades', 'fldFlag')}]" \
    #         f" ) VALUES ({intIdAnimalesRegistroActividades}, {intIdAnimal}, 1, '{strTimeStamp}'," \
    #         f" 1, 0 " \
    #         f" );"
    #     print(strSQLAnimalesRegistroActividades)
    #     # [Data Animales Categorias] table
    #     # Hardcoded values:
    #     #   [Cambio Generado Por Sistema] → 0 (No generado por sistema)
    #     intIdDataAnimalesCategorias = maxId('tblDataAnimalesCategorias') + 1
    #     strSQLDataAnimalesCategorias = f"" \
    #         f"INSERT INTO {tblBr('tblDataAnimalesCategorias')} (" \
    #         f" [{fld('tblDataAnimalesCategorias', 'fldID')}]," \
    #         f" [{fld('tblDataAnimalesCategorias', 'fldFK_Actividad')}]," \
    #         f" [{fld('tblDataAnimalesCategorias', 'fldFK_Categoria')}]," \
    #         f" [{fld('tblDataAnimalesCategorias', 'fldModifiedBySystem')}]," \
    #         f" [{fld('tblDataAnimalesCategorias', 'fldComment')}]" \
    #         f" ) VALUES ({intIdDataAnimalesCategorias}, {intIdAnimalesRegistroActividades}," \
    #         f" {intCategoria}, 0, 'Categoría de ingreso'" \
    #         f" );"
    #     print(strSQLDataAnimalesCategorias)
    #     # [Data Animales Actividad Alta] table
    #     intIdDataAnimalesActividadAlta = maxId('tblDataAnimalesActividadAlta') + 1
    #     strSQLDataAnimalesActividadAlta = f"" \
    #         f"INSERT INTO {tblBr('tblDataAnimalesActividadAlta')} (" \
    #         f" [{fld('tblDataAnimalesActividadAlta', 'fldID')}]," \
    #         f" [{fld('tblDataAnimalesActividadAlta', 'fldFK_Actividad')}]," \
    #         f" [{fld('tblDataAnimalesActividadAlta', 'fldFK_TipoDeAlta/Baja')}]" \
    #         f" ) VALUES ({intIdDataAnimalesActividadAlta}, {intIdAnimalesRegistroActividades}," \
    #         f" {intTipoEntrada}" \
    #         f" );"
    #     print(strSQLDataAnimalesActividadAlta)
    #     # Tags
    #     # print('IDs de caravana: ' + self.lstIdCaravanas.__repr__())
    #     if len(self.lstIdCaravanas) > 0:
    #         intIdCaravanasRegistroDeActividades = maxId('tblCaravanasRegistroDeActividades')
    #         intIdDataAnimalesActividadCaravanas = maxId('tblDataAnimalesActividadCaravanas')
    #         query.exec_('BEGIN TRANSACTION;')
    #         for idTag in self.lstIdCaravanas:
    #
    #             # [Caravanas Registro De Actividades] table
    #             # Hardcoded values:
    #             #   [ID_Nombre Actividad] → 3 (Comisión)
    #             #   [ID_Status Caravana] → 2 (Asignada)
    #             #   [Inventario] → 1 (Es actividad de inventario)
    #             intIdCaravanasRegistroDeActividades += 1
    #             strSQLCaravanasRegistroDeActividades = f"" \
    #                 f"INSERT INTO {tblBr('tblCaravanasRegistroDeActividades')} (" \
    #                 f" [{fld('tblCaravanasRegistroDeActividades', 'fldID')}]," \
    #                 f" [{fld('tblCaravanasRegistroDeActividades', 'fldFK_Caravana')}]," \
    #                 f" [{fld('tblCaravanasRegistroDeActividades', 'fldFK_NombreActividad')}]," \
    #                 f" [{fld('tblCaravanasRegistroDeActividades', 'fldTimeStamp')}]," \
    #                 f" [{fld('tblCaravanasRegistroDeActividades', 'fldFK_StatusCaravana')}]," \
    #                 f" [{fld('tblCaravanasRegistroDeActividades', 'fldInventory')}]" \
    #                 f" ) VALUES ({intIdCaravanasRegistroDeActividades}, {int(idTag)}, 3, '{strTimeStamp}', 2, 1" \
    #                 f" );"
    #             print(strSQLCaravanasRegistroDeActividades)
    #             query.exec_(strSQLCaravanasRegistroDeActividades)
    #             # [Data Animales Actividad Caravanas] table
    #             intIdDataAnimalesActividadCaravanas += 1
    #             strSQLDataAnimalesActividadCaravanas = f"" \
    #                 f"INSERT INTO {tblBr('tblDataAnimalesActividadCaravanas')} (" \
    #                 f" [{fld('tblDataAnimalesActividadCaravanas', 'fldID')}]," \
    #                 f" [{fld('tblDataAnimalesActividadCaravanas', 'fldFK_Actividad')}]," \
    #                 f" [{fld('tblDataAnimalesActividadCaravanas', 'fldFK_ActividadCaravana')}]" \
    #                 f" ) VALUES ({intIdDataAnimalesActividadCaravanas}, {intIdAnimalesRegistroActividades}," \
    #                 f" {intIdCaravanasRegistroDeActividades}" \
    #                 f" );"
    #             print(strSQLDataAnimalesActividadCaravanas)
    #             query.exec_(strSQLDataAnimalesActividadCaravanas)
    #         query.exec_('COMMIT;')
    #     # [Data Animales Actividad Castracion] table
    #     if blnCastr:
    #         # WARNING: [FechaHora Castracion] field is not the same as strTimeStamp
    #         # Hardcoded values:
    #         #   [Castrado] → 1 (Castrado)
    #         intIdDataAnimalesActividadCastracion = maxId('tblDataAnimalesActividadCastracion') + 1
    #         strSQLDataAnimalesActividadCastracion = f"" \
    #             f"INSERT INTO {tblBr('tblDataAnimalesActividadCastracion')} (" \
    #             f" [{fld('tblDataAnimalesActividadCastracion', 'fldID')}]," \
    #             f" [{fld('tblDataAnimalesActividadCastracion', 'fldFK')}]," \
    #             f" [{fld('tblDataAnimalesActividadCastracion', 'fldTimeStamp')}]," \
    #             f" [{fld('tblDataAnimalesActividadCastracion', 'fldCastrado')}]" \
    #             f" ) VALUES ({intIdDataAnimalesActividadCastracion}, {intIdAnimalesRegistroActividades}," \
    #             f"  '--fldTimeStamp--', 1" \
    #             f" );"
    #     else:
    #         strSQLDataAnimalesActividadCastracion = None
    #     print(strSQLDataAnimalesActividadCastracion)
    #     # [Data Animales Actividad Preñez] table
    #     if blnPregn:
    #         # Hardcoded value:
    #         #   [Preñez] → 1 (Preñada)
    #         intIdDataAnimalesActividadPreniez = maxId('tblDataAnimalesActividadPreñez') + 1
    #         strSQLDataAnimalesActividadPreniez = f"" \
    #             f"INSERT INTO {tblBr('tblDataAnimalesActividadPreñez')} (" \
    #             f" [{fld('tblDataAnimalesActividadPreñez', 'fldID')}]," \
    #             f" [{fld('tblDataAnimalesActividadPreñez', 'fldFK')}]," \
    #             f" [{fld('tblDataAnimalesActividadPreñez', 'fldPregnant')}]" \
    #             f" ) VALUES ({intIdDataAnimalesActividadPreniez}, {intIdAnimalesRegistroActividades}, 1" \
    #             f" );"
    #     else:
    #         strSQLDataAnimalesActividadPreniez = None
    #     print(strSQLDataAnimalesActividadPreniez)
    #     # [Data Animales Actividad Pesaje] table
    #     if self.dblPeso and self.dblPeso > 0:
    #         # Hardcoded values:
    #         #   [ID_Unidad] → 1 (Kg.)
    #         intIdDataAnimalesActividadPesaje = maxId('tblDataAnimalesActividadPesaje') + 1
    #         strSQLDataAnimalesActividadPesaje = f"" \
    #             f"INSERT INTO {tblBr('tblDataAnimalesActividadPesaje')} (" \
    #             f" [{fld('tblDataAnimalesActividadPesaje', 'fldID')}]," \
    #             f" [{fld('tblDataAnimalesActividadPesaje', 'fldFK_Actividad')}]," \
    #             f" [{fld('tblDataAnimalesActividadPesaje', 'fldWeight')}]," \
    #             f" [{fld('tblDataAnimalesActividadPesaje', 'fldFK_Unidad')}]" \
    #             f" ) VALUES ({intIdDataAnimalesActividadPesaje}, {intIdAnimalesRegistroActividades}," \
    #             f" {self.dblPeso}, 1" \
    #             f" );"
    #     else:
    #         strSQLDataAnimalesActividadPesaje = None
    #     print(strSQLDataAnimalesActividadPesaje)
    #     # [Data Animales Actividad Destete] table
    #     if self.qdtFechaDestete:
    #         intIdDataAnimalesActividadDestete = maxId('tblDataAnimalesActividadDestete') + 1
    #         strFechaDestete = "\'" + self.qdtFechaDestete.toPyDate().isoformat() + "\'"
    #         strSQLDataAnimalesActividadDestete = f"" \
    #             f"INSERT INTO {tblBr('tblDataAnimalesActividadDestete')} (" \
    #             f" [{fld('tblDataAnimalesActividadDestete', 'fldID')}]," \
    #             f" [{fld('tblDataAnimalesActividadDestete', 'fldFK')}]," \
    #             f" [{fld('tblDataAnimalesActividadDestete', 'fldTimeStamp')}]" \
    #             f" ) VALUES ({intIdDataAnimalesActividadDestete}, {intIdAnimalesRegistroActividades}," \
    #             f" {strFechaDestete}" \
    #             f" );"
    #     else:
    #         strSQLDataAnimalesActividadDestete = None
    #     print(strSQLDataAnimalesActividadDestete)
    #     # [Data Animales Status] table
    #     # Hardcoded values:
    #     #   [ID_Status Animal] → 1 (En Stock)
    #     intIdDataAnimalesStatus = maxId('tblDataAnimalesStatus') + 1
    #     strSQLDataAnimalesStatus = f"" \
    #         f"INSERT INTO {tblBr('tblDataAnimalesStatus')} (" \
    #         f" [{fld('tblDataAnimalesStatus', 'fldID')}]," \
    #         f" [{fld('tblDataAnimalesStatus', 'fldFK_Actividad')}]," \
    #         f" [{fld('tblDataAnimalesStatus', 'fldFK_StatusAnimal')}]" \
    #         f" ) VALUES ({intIdDataAnimalesStatus}, {intIdAnimalesRegistroActividades}, 1" \
    #         f" );"
    #     print(strSQLDataAnimalesStatus)
    #     # [Data Animales Actividad Marca] table
    #     intIdDataAnimalesMarca = maxId('tblDataAnimalesActividadMarca') + 1
    #     strSQLDataAnimalesMarca = f"" \
    #         f"INSERT INTO {tblBr('tblDataAnimalesActividadMarca')} (" \
    #         f" [{fld('tblDataAnimalesActividadMarca', 'fldID')}]," \
    #         f" [{fld('tblDataAnimalesActividadMarca', 'fldFK_Actividad')}]," \
    #         f" [{fld('tblDataAnimalesActividadMarca', 'fldFK_Marca')}]" \
    #         f" ) VALUES ({intIdDataAnimalesMarca}, {intIdAnimalesRegistroActividades}, {intMarca}" \
    #         f" );"
    #     print(strSQLDataAnimalesMarca)
    #     # [Data Animales Actividad Asignacion De Dueño] table
    #     intIdDataAnimalesDuenio = maxId('tblDataAnimalesActividadAsignacionDeDueño') + 1
    #     strSQLDataAnimalesDuenio = f"" \
    #         f"INSERT INTO {tblBr('tblDataAnimalesActividadAsignacionDeDueño')} (" \
    #         f" [{fld('tblDataAnimalesActividadAsignacionDeDueño', 'fldID')}]," \
    #         f" [{fld('tblDataAnimalesActividadAsignacionDeDueño', 'fldFK_Actividad')}]," \
    #         f" [{fld('tblDataAnimalesActividadAsignacionDeDueño', 'fldFK_PersonaDueño')}]" \
    #         f" ) VALUES ({intIdDataAnimalesDuenio}, {intIdAnimalesRegistroActividades}, {intDuenio}" \
    #         f" );"
    #     print(strSQLDataAnimalesDuenio)
    #     # [Geo Registro De Localizacion] table
    #     # Hardcoded values:
    #     #   [ID_Nivel De Localizacion] → 300 (Potrero)
    #     #   [ID_Pais] → 1 (Argentina)
    #     #   [ID_Provincia] → 20 (Santa Fe)
    #     #   [ID_Region Provincia] → -1
    #     #   [ID_Departamento] → 198 (Nueve de Julio)
    #     #   [ID_Link Establecimiento_Provincia] → -1
    #     #   [ID_Link Lote_Departamento] → -1
    #     intIdGeoRegistroDeLocalizacion = maxId('tblGeoRegistroDeLocalizacion') + 1
    #     strSQLGeoRegistroDeLocalizacion = f"" \
    #         f"INSERT INTO {tblBr('tblGeoRegistroDeLocalizacion')}" \
    #         f" VALUES ({intIdGeoRegistroDeLocalizacion}, 300, '{strTimeStamp}', 1, 20, -1, 198, {intEstablecimiento}," \
    #         f"  -1, {self.intLote}, -1, {self.intPotrero}, {self.intLocacion}," \
    #         f" 'Localización de ingreso'" \
    #         f" );"
    #     print(strSQLGeoRegistroDeLocalizacion)
    #     # [Data Animales Actividad Localizacion] table
    #     intIdDataAnimalesLocalizacion = maxId('tblDataAnimalesActividadLocalizacion') + 1
    #     strSQLDataAnimalesLocalizacion = f"" \
    #         f"INSERT INTO {tblBr('tblDataAnimalesActividadLocalizacion')}" \
    #         f" VALUES ({intIdDataAnimalesLocalizacion}, {intIdAnimalesRegistroActividades}," \
    #         f" {intIdGeoRegistroDeLocalizacion}, 'Localización de ingreso'" \
    #         f" );"
    #     print(strSQLDataAnimalesLocalizacion)
    #
    #     # Insert values
    #     query.exec_('BEGIN TRANSACTION;')
    #     query.exec_(strSQLAnimales)
    #     query.exec_(strSQLAnimalesRegistroActividades)
    #     query.exec_(strSQLDataAnimalesCategorias)
    #     query.exec_(strSQLDataAnimalesActividadAlta)
    #     if strSQLDataAnimalesActividadCastracion:
    #         query.exec_(strSQLDataAnimalesActividadCastracion)
    #     if strSQLDataAnimalesActividadPreniez:
    #         query.exec_(strSQLDataAnimalesActividadPreniez)
    #     if strSQLDataAnimalesActividadPesaje:
    #         query.exec_(strSQLDataAnimalesActividadPesaje)
    #     if strSQLDataAnimalesActividadDestete:
    #         query.exec_(strSQLDataAnimalesActividadDestete)
    #     query.exec_(strSQLDataAnimalesStatus)
    #     query.exec_(strSQLDataAnimalesMarca)
    #     query.exec_(strSQLDataAnimalesDuenio)
    #     query.exec_(strSQLGeoRegistroDeLocalizacion)
    #     query.exec_(strSQLDataAnimalesLocalizacion)
    #     query.exec_('COMMIT;')
    #
    #     # 4) Cleanup:
    #     #    - Close connections and destroy objects
    #     if self.dbGanadoConnCreated:
    #         self.db.close()
    #         QSqlDatabase.removeDatabase("GanadoSQLite.db")
    #     self.accept()
    #     # self.close()


# Usage
if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setStyle('fusion')
    dialog = DialogoInsertarAnimales()
    # app.focusChanged.connect(dialog.onFocusChanged)

    dialog.show()
    sys.exit(app.exec_())
