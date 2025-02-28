import sys
from PyQt6 import QtCore, QtWidgets, QtGui
# https://stackoverflow.com/questions/52560496/getting-a-second-window-pass-a-variable-to-the-main-ui-and-close
from ui.IngresoAnimalesTabbed import Ui_dlgIngresoAnimales
from ui_alta_caravanas import UiAltaCaravanas
import krnl_db_query                    # Decorators for read_sql_query,
from krnl_tag import Tag
import pandas as pd
import numpy as np
from krnl_db_query import SQLiteQuery
from krnl_custom_types import getRecords, getrecords
from krnl_abstract_class_animal import Animal
from krnl_bovine import Bovine
from datetime import datetime, timedelta

from krnl_custom_types import DataTable, getRecords, dbRead


class UiAltaAnimales():
    def __init__(self, parent=None):

        dlgAlta = DialogAltaAnimales()
        # app.setStyle('Windows')  # 'windowsvista', 'Windows', 'Fusion'
        if dlgAlta.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            # set result variables
            dlgAlta.intTipoEntrada = dlgAlta.cboTipoDeEntrada.currentData()
            dlgAlta.intCategoria = dlgAlta.cboCategoria.currentData()
            dlgAlta.intRaza = dlgAlta.cboRaza.currentData()
            dlgAlta.intEstablecimiento = dlgAlta.cboEstablecimiento.currentData()
            dlgAlta.intDuenio = dlgAlta.cboDuenio.currentData()
            dlgAlta.intMarca = dlgAlta.cboMarca.currentData()
            dlgAlta.blnPregn = dlgAlta.chkPreniez.isChecked()
            dlgAlta.strComentario = dlgAlta.pteComentario.toPlainText()
            dlgAlta.qdtFechaNac = dlgAlta.datNacimiento.date()
            dlgAlta.qdtFechaDestete = dlgAlta.datDestete.date()
            dlgAlta.qdtFechaServ = dlgAlta.datServicio.date()
            dlgAlta.dblPeso = dlgAlta.dspPeso.value()
            dlgAlta.intLote = dlgAlta.cboLote.currentData()
            dlgAlta.intPotrero = dlgAlta.cboPotrero.currentData()
            dlgAlta.intLocacion = dlgAlta.cboLocacion.currentData()
            dlgAlta.blnTratamientos = dlgAlta.chkTratamientos.isChecked()
            dlgAlta.blnDieta = dlgAlta.chkDieta.isChecked()
            # TEMP
            print('Tab 1:')
            print('    intTipoEntrada: ' + str(dlgAlta.intTipoEntrada))
            print('    intCategoria: ' + str(dlgAlta.intCategoria))
            print('    intRaza: ' + str(dlgAlta.intRaza))
            print('    intEstablecimiento: ' + str(dlgAlta.intEstablecimiento))
            print('    intDuenio: ' + str(dlgAlta.intDuenio))
            print('    intMarca: ' + str(dlgAlta.intMarca))
            print('    blnPregn: ' + str(dlgAlta.blnPregn))
            print('    strComentario: ' + str(dlgAlta.strComentario))
            print('Tab 2:')
            print('    lstCaravanasRight: ')
            print(dlgAlta.lstCaravanasRight)
            print('Tab 3:')
            print('    qdtFechaNac: ' + str(dlgAlta.qdtFechaNac))
            print('    qdtFechaDestete: ' + str(dlgAlta.qdtFechaDestete))
            print('    qdtFechaServ: ' + str(dlgAlta.qdtFechaServ))
            print('    dblPeso: ' + str(dlgAlta.dblPeso))
            print('    intLote: ' + str(dlgAlta.intLote))
            print('    intPotrero: ' + str(dlgAlta.intPotrero))
            print('    intLocacion: ' + str(dlgAlta.intLocacion))
            print('    blnTratamientos: ' + str(dlgAlta.blnTratamientos))
            print('    blnDieta: ' + str(dlgAlta.blnDieta))
        else:
            print('Rejected')


class DialogAltaAnimales(QtWidgets.QDialog, Ui_dlgIngresoAnimales):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi(self)

        # initialize result variables
        self.intTipoEntrada = 1  # YA
        self.intCategoria = 1  # YA
        self.intRaza = 1  # YA
        self.intEstablecimiento = 1  # YA
        self.intDuenio = None  # YA
        self.intMarca = None  # YA
        self.blnPregn = True  # YA
        self.strComentario = '' # YA
        self.lstIdCaravanas = []
        self.qdtFechaNac = QtCore.QDate.currentDate().addMonths(-24)
        self.qdtFechaDestete = QtCore.QDate.currentDate().addMonths(-18)
        self.qdtFechaServ = QtCore.QDate.currentDate().addMonths(-3)
        self.dblPeso = 150.0
        self.intLote = None
        self.intPotrero = None
        self.intLocacion = None
        self.blnTratamientos = False
        self.blnDieta = False

        self.currentFarm = None
        self.currentFarmUID = None
        self.currentField = None
        self.currentFieldUID = None

        # create lists of values
        self.dfTemp = getrecords('tblAnimalesTiposDeAltaBaja','fldName', 'fldID')
        self.lstTipoAlta = sorted(list(zip(self.dfTemp.fldName.values,  self.dfTemp.fldID.values)))
        self.dfTemp = getrecords('tblAnimalesCategorias','fldName', 'fldID')
        self.lstCategorias = sorted(list(zip(self.dfTemp.fldName.values,  self.dfTemp.fldID.values)))
        self.dfTemp = getrecords('tblAnimalesRazas','fldName', 'fldID')
        self.lstRazas = sorted(list(zip(self.dfTemp.fldName.values,  self.dfTemp.fldID.values)))
        self.dfTemp = pd.read_sql_query('SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades" WHERE'
                    ' "ID_Nivel De Localizacion" = 40'
                    ' AND "GUI_Bitmask" = 1 ORDER BY "Nombre Entidad"', SQLiteQuery().conn)
        self.lstEstablecimientos = sorted(list(zip(self.dfTemp.fldName.values, self.dfTemp.fldID.values)))
        self.dfTemp = getrecords('tblPersonas','fldName', 'fldID')
        self.lstPersonasTemp = sorted(list(zip(self.dfTemp.fldName.values,  self.dfTemp.fldID.values)))
        self.lstPersonasTemp = getRecords('tblPersonas', '', '', None, 'fldLastName', 'fldName', 'fldID').dataList
        self.lstPersonas = []
        for itm in self.lstPersonasTemp:
            if itm[2] in (1, 4):
                continue
            self.lstPersonas.append([itm[0] + ' ' + itm[1], itm[2]])
        self.dfTemp = getrecords('tblAnimalesMarcas','fldName', 'fldID')
        self.lstMarcas = sorted(list(zip(self.dfTemp.fldName.values,  self.dfTemp.fldID.values)))
        self.dfTemp = getrecords('tblCaravanas','fldTagNumber', 'fldID')

        self.dftags = pd.read_sql_query(f'SELECT * FROM {Tag.tblObjDBName()};', SQLiteQuery().conn)
        self.tags_taken = Tag.get_tags_in_use()
        self.available_tags = self.dftags[~self.dftags.fldObjectUID.isin(self.tags_taken)]
        self.tplCaravanasLeft = sorted(list(zip(self.available_tags.fldTagNumber.values,  self.available_tags.fldObjectUID.values)))
        self.lstCaravanasLeft = [list(elem) for elem in self.tplCaravanasLeft]
        self.tplCaravanasRight = []
        self.lstCaravanasRight = []


        self.dspPeso.setValue(self.dblPeso)
        self.chkPreniez.setVisible(False)

        # populate combo boxes
        for item, intId in self.lstTipoAlta:
            self.cboTipoDeEntrada.addItem(item, intId)
        for item, intId in self.lstCategorias:
            self.cboCategoria.addItem(item, intId)
        for item, intId in self.lstRazas:
            self.cboRaza.addItem(item, intId)
        for item, intId in self.lstEstablecimientos:
            self.cboEstablecimiento.addItem(item, intId)
        self.cboEstablecimiento.setCurrentIndex(1)
        for item, intId in self.lstPersonas:
            self.cboDuenio.addItem(item, intId)
        for item, intId in self.lstMarcas:
            self.cboMarca.addItem(item, intId)
        self.establecimientoChanged()
        self.loteChanged()

        # format tables
        self.tableWidgetLeft.setColumnCount(2)
        self.tableWidgetLeft.setColumnHidden(1,True)
        self.tableWidgetLeft.setColumnWidth(0, 265)
        self.tableWidgetRight.setColumnCount(2)
        self.tableWidgetRight.setColumnHidden(1,True)
        self.tableWidgetRight.setColumnWidth(0, 265)

        # populate controls
        self.datNacimiento.setDate(self.qdtFechaNac)
        self.datDestete.setDate(self.qdtFechaDestete)
        self.datServicio.setDate(self.qdtFechaServ)
        self.updateTagTables()

        # signals & slots
        self.btnOk.clicked.connect(self.accept)
        self.btnCancel.clicked.connect(self.reject)
        self.btnNewTag.clicked.connect(self.newTag)
        self.cboCategoria.currentIndexChanged.connect(self.categoriaChanged)
        self.cboEstablecimiento.currentIndexChanged.connect(self.establecimientoChanged)
        self.cboLote.currentIndexChanged.connect(self.loteChanged)
        self.btnToRight.clicked.connect(self.toRight)
        self.btnToLeft.clicked.connect(self.toLeft)


    def newTag(self):
        try:
            tag = UiAltaCaravanas()
        except (RuntimeError, Exception):
            tag = None
        if tag is not None:
            mytag = tag.tag_obj
            tagNum = mytag.tagNumber
            tagID = mytag.ID
            self.lstCaravanasRight.append([tagNum, tagID])
            self.updateTagTables()


    def updateTagTables(self):
        self.tableWidgetLeft.setRowCount(len(self.lstCaravanasLeft))
        self.tableWidgetRight.setRowCount(len(self.lstCaravanasRight))
        for i, (name, code) in enumerate(self.lstCaravanasLeft):
            item_name = QtWidgets.QTableWidgetItem(name)
            item_code = QtWidgets.QTableWidgetItem(code)
            self.tableWidgetLeft.setItem(i, 0, item_name)
            self.tableWidgetLeft.setItem(i, 1, item_code)
        for i, (name, code) in enumerate(self.lstCaravanasRight):
            item_name = QtWidgets.QTableWidgetItem(name)
            item_code = QtWidgets.QTableWidgetItem(code)
            self.tableWidgetRight.setItem(i, 0, item_name)
            self.tableWidgetRight.setItem(i, 1, item_code)


    def toRight(self):
        for itm in self.tableWidgetLeft.selectedItems():
            print(self.tableWidgetLeft.item(itm.row(), 0).text(), self.tableWidgetLeft.item(itm.row(), 1).text())
            tagNum = self.tableWidgetLeft.item(itm.row(), 0).text()
            tagID = self.tableWidgetLeft.item(itm.row(), 1).text()
            self.lstCaravanasLeft = [itm for itm in self.lstCaravanasLeft if itm[1] != tagID]
            self.lstCaravanasRight.append([tagNum, tagID])
            self.lstCaravanasRight.sort()
        self.updateTagTables()
        self.tableWidgetLeft.clearSelection()


    def toLeft(self):
        for itm in self.tableWidgetRight.selectedItems():
            print(self.tableWidgetRight.item(itm.row(), 0).text(), self.tableWidgetRight.item(itm.row(), 1).text())
            tagNum = self.tableWidgetRight.item(itm.row(), 0).text()
            tagID = self.tableWidgetRight.item(itm.row(), 1).text()
            self.lstCaravanasRight = [itm for itm in self.lstCaravanasRight if itm[1] != tagID]
            self.lstCaravanasLeft.append([tagNum, tagID])
            self.lstCaravanasLeft.sort()
        self.updateTagTables()
        self.tableWidgetRight.clearSelection()


    def categoriaChanged(self):
        if self.cboCategoria.currentData() in [2, 3, 4]:
            self.chkPreniez.setVisible(True)
        else:
            self.chkPreniez.setVisible(False)


    def establecimientoChanged(self):
        self.currentFarm = self.cboEstablecimiento.currentData()
        self.lstLotes = []
        if self.currentFarm:
            self.dfTemp = pd.read_sql_query(f'SELECT "UID_Objeto" FROM "Geo Entidades" WHERE "ID_GeoEntidad" = {self.currentFarm}', SQLiteQuery().conn)
            self.currentFarmUID = list(zip(self.dfTemp.fldObjectUID.values))[0][0]
            self.dfTemp = pd.read_sql_query(f'SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades"'
                                   f' WHERE "ID_Nivel De Localizacion" = 50 AND "Containers" LIKE "%{self.currentFarmUID}%"'
                                   f' AND "GUI_Bitmask" = 1 ORDER BY "Nombre Entidad"', SQLiteQuery().conn)
            self.lstLotes = sorted(list(zip(self.dfTemp.fldName.values,  self.dfTemp.fldID.values)))

        # populate combo box
        self.cboLote.clear()
        if self.lstLotes == []:
            self.cboLote.setEnabled(False)
        else:
            self.lstLotes.insert(0, ['', None])
            for item, intId in self.lstLotes:
                self.cboLote.addItem(item, intId)
            self.cboLote.setEnabled(True)

        self.cboPotrero.clear()
        self.cboLocacion.clear()

    def loteChanged(self):
        # if chosen field has potreros, enable cboPotrero and load items
        # if chosen field has locations, enable cboLocacion and load items
        self.currentField = self.cboLote.currentData()
        self.lstPotreros = []
        self.lstLocaciones = []
        self.cboPotrero.clear()
        self.cboPotrero.setEnabled(False)
        self.cboLocacion.clear()
        self.cboLocacion.setEnabled(False)
        if self.currentField:
            self.cboPotrero.clear()
            self.dfTemp = pd.read_sql_query(f'SELECT "UID_Objeto" FROM "Geo Entidades" WHERE "ID_GeoEntidad" = {self.currentField}', SQLiteQuery().conn)
            self.currentFieldUID = list(zip(self.dfTemp.fldObjectUID.values))[0][0]
            # create lists of values for combo boxes
            self.dfTemp = pd.read_sql_query(f'SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades"'
                                   f' WHERE "ID_Nivel De Localizacion" = 60 AND "Containers" LIKE "%{self.currentFieldUID}%"'
                                   f' AND "GUI_Bitmask" = 1 ORDER BY "Nombre Entidad"', SQLiteQuery().conn)
            self.lstPotreros = sorted(list(zip(self.dfTemp.fldName.values, self.dfTemp.fldID.values)))
            self.dfTemp = pd.read_sql_query(f'SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades"'
                                   f' WHERE "ID_Nivel De Localizacion" = 70 AND "Containers" LIKE "%{self.currentFieldUID}%"'
                                   f' AND "GUI_Bitmask" = 1 ORDER BY "Nombre Entidad"', SQLiteQuery().conn)
            self.lstLocaciones = sorted(list(zip(self.dfTemp.fldName.values, self.dfTemp.fldID.values)))
            # populate combo boxes
            if self.lstPotreros == []:
                self.cboPotrero.clear()
                self.cboPotrero.setEnabled(False)
            else:
                self.lstPotreros.insert(0, ['', None])
                for item, intId in self.lstPotreros:
                    self.cboPotrero.addItem(item, intId)
                self.cboPotrero.setEnabled(True)
            if self.lstLocaciones == []:
                self.cboLocacion.clear()
                self.cboLocacion.setEnabled(False)
            else:
                self.lstLocaciones.insert(0, ['', None])
                for item, intId in self.lstLocaciones:
                    self.cboLocacion.addItem(item, intId)
                self.cboLocacion.setEnabled(True)



if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    alta = UiAltaAnimales()
    del alta
