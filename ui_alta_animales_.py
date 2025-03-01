import sys
from PyQt6 import QtCore, QtWidgets
# https://stackoverflow.com/questions/52560496/getting-a-second-window-pass-a-variable-to-the-main-ui-and-close

from ui.IngresoAnimales import Ui_dlgIngresoAnimales
from ui.IngresoAnimalesAdic import Ui_dlgIngresoAnimalesAdic
from ui.IngresoAnimalesCaravanas import Ui_dlgIngresoAnimalesCaravanas
import krnl_db_query                    # Decorators for read_sql_query,
from krnl_tag import Tag
import pandas as pd
import numpy as np
from krnl_db_query import SQLiteQuery
from krnl_custom_types import getRecords, getrecords
from krnl_abstract_class_animal import Animal
from krnl_bovine import Bovine
from krnl_custom_types import DataTable, getRecords, dbRead


class UiAltaAnimales():
    def __init__(self, parent=None):

        app = QtWidgets.QApplication(sys.argv)
        dlgAlta = DialogAltaAnimales()
        if dlgAlta.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            # set result variables
            dlgAlta.intTipoEntrada = dlgAlta.cboTipoDeEntrada.currentData()
            dlgAlta.intCategoria = dlgAlta.cboCategoria.currentData()
            dlgAlta.intRaza = dlgAlta.cboRaza.currentData()
            dlgAlta.intEstablecimiento = dlgAlta.cboEstablecimiento.currentData()
            dlgAlta.intDuenio = dlgAlta.cboDuenio.currentData()
            dlgAlta.intMarca = dlgAlta.cboMarca.currentData()
            # TEMP
            print('Alta Animales:')
            print('    intTipoEntrada: ' + str(dlgAlta.intTipoEntrada))
            print('    intCategoria: ' + str(dlgAlta.intCategoria))
            print('    intRaza: ' + str(dlgAlta.intRaza))
            print('    intEstablecimiento: ' + str(dlgAlta.intEstablecimiento))
            print('    intDuenio: ' + str(dlgAlta.intDuenio))
            print('    intMarca: ' + str(dlgAlta.intMarca))
        else:
            print('Rejected')


class DialogAltaAnimales(QtWidgets.QDialog, Ui_dlgIngresoAnimales):

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi(self)

        # initialize result variables
        self.intTipoEntrada = 1
        self.intCategoria = 1
        self.intRaza = 1
        self.intEstablecimiento = 1
        self.intDuenio = 1
        self.intMarca = 1
        self.blnPregn = True
        self.strComentario = ''
        self.lstIdCaravanas = []
        self.qdtFechaNac = None
        self.qdtFechaDestete = None
        self.qdtFechaServ = None
        self.dblPeso = 50.0
        self.intLote = 1
        self.intPotrero = 1
        self.intLocacion = 1
        self.blnTratamientos = False
        self.blnDieta = False

        # create lists of values for combo boxes
        self.dfTemp = getrecords('tblAnimalesTiposDeAltaBaja','fldName', 'fldID')
        self.lstTipoAlta = sorted(list(zip(self.dfTemp.fldName.values,  self.dfTemp.fldID.values)))
        # self.lstTipoAlta = dbRead('tblAnimalesTiposDeAltaBaja',
        #             'SELECT "Nombre Tipo De AltaBaja", "ID_Tipo De AltaBaja" FROM "Animales Tipos De AltaBaja"'
        #             ' WHERE "AltaBaja" = "Alta" ORDER BY "Nombre Tipo De AltaBaja"', 0).dataList
        self.dfTemp = getrecords('tblAnimalesCategorias','fldName', 'fldID')
        self.lstCategorias = sorted(list(zip(self.dfTemp.fldName.values,  self.dfTemp.fldID.values)))
        # self.lstCategorias = dbRead('tblAnimalesCategorias',
        #             'SELECT "Nombre Categoria", "ID_Categoria" FROM "Animales Categorias" WHERE'
        #             ' "ID_Clase De Animal" = 1 ORDER BY "Nombre Categoria"', 0).dataList
        self.dfTemp = getrecords('tblAnimalesRazas','fldName', 'fldID')
        self.lstRazas = sorted(list(zip(self.dfTemp.fldName.values,  self.dfTemp.fldID.values)))
        # self.lstRazas = getRecords('tblAnimalesRazas', '', '', None, 'fldName', 'fldID').dataList
        self.dfTemp = pd.read_sql_query('SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades" WHERE'
                    ' "ID_Nivel De Localizacion" = 40'
                    ' ORDER BY "Nombre Entidad"', SQLiteQuery().conn)
        self.lstEstablecimientos = sorted(list(zip(self.dfTemp.fldName.values, self.dfTemp.fldID.values)))
        # self.lstEstablecimientos = dbRead('tblGeoEntidades',
        #             'SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades" WHERE'
        #             ' "ID_Nivel De Localizacion" = 40'
        #             ' ORDER BY "Nombre Entidad"', 0).dataList   #  AND NOT "ID_GeoEntidad" = 2
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
        self.lstMarcas = getRecords('tblAnimalesMarcas', '', '', None, 'fldName', 'fldID').dataList

        # populate combo boxes
        for item, intId in self.lstTipoAlta:
            self.cboTipoDeEntrada.addItem(item, str(intId))
        for item, intId in self.lstCategorias:
            self.cboCategoria.addItem(item, str(intId))
        for item, intId in self.lstRazas:
            self.cboRaza.addItem(item, str(intId))
        for item, intId in self.lstEstablecimientos:
            if intId == 2:
                item = '----'
            self.cboEstablecimiento.addItem(item, str(intId))
        self.nullFarmIdx = self.cboEstablecimiento.findText('----')
        if self.nullFarmIdx > -1:
            self.cboEstablecimiento.setCurrentIndex(self.nullFarmIdx)
        for item, intId in self.lstPersonas:
            self.cboDuenio.addItem(item, str(intId))
        for item, intId in self.lstMarcas:
            self.cboMarca.addItem(item, str(intId))

        # signals & slots
        self.btnAdic.clicked.connect(self.dialogAdic)
        self.btnCaravanas.clicked.connect(self.dialogCaravanas)
        self.btnOk.clicked.connect(self.accept)
        self.btnCancel.clicked.connect(self.reject)

    def dialogAdic(self):   # show additional information dialog
        dlgAdic = DialogAltaAnimalesAdic()
        self.currentFarm = self.cboEstablecimiento.currentData()
        if self.currentFarm == '2':
            self.currentFarmUID = None
            dlgAdic.cboLote.setEnabled(False)
        else:
            self.currentFarmUID = dbRead('tblGeoEntidades',
                                         f'SELECT "UID_Objeto" FROM "Geo Entidades" WHERE "ID_GeoEntidad" = {self.currentFarm}', 0).dataList[0][0]
            # create lists of values for 'cboLote' combo box
            self.lstLotes = dbRead('tblGeoEntidades',
                                   f'SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades"'
                                   f' WHERE "ID_Nivel De Localizacion" = 50 AND "Containers" = \'"{self.currentFarmUID}"\''
                                   f' ORDER BY "Nombre Entidad"', 0).dataList
            dlgAdic.cboLote.setEnabled(True)
            for item, intId in self.lstLotes:
                dlgAdic.cboLote.addItem(item, str(intId))
        dlgAdic.cboPotrero.setEnabled(False)
        dlgAdic.cboLocacion.setEnabled(False)
        dlgAdic.dspPeso.setValue(self.dblPeso)

        # additional signals & slots

        if dlgAdic.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            self.qdtFechaNac = None
            self.qdtFechaDestete = None
            self.qdtFechaServ = None
            self.dblPeso = dlgAdic.dspPeso.value()
            self.intLote = 1
            self.intPotrero = 1
            self.intLocacion = 1
            self.blnTratamientos = False
            self.blnDieta = False
            print('Accepted')
        else:
            print('Rejected')

    def dialogCaravanas(self):  # show tag selection dialog
        dlgCaravanas = DialogAltaAnimalesCaravanas()
        if dlgCaravanas.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            print('Accepted')
            self.lstIdCaravanas = []
        else:
            print('Rejected')


class DialogAltaAnimalesAdic(QtWidgets.QDialog, Ui_dlgIngresoAnimalesAdic):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi(self)

        # signals & slots
        self.btnOk.clicked.connect(self.accept)
        self.btnCancel.clicked.connect(self.reject)


class DialogAltaAnimalesCaravanas(QtWidgets.QDialog, Ui_dlgIngresoAnimalesCaravanas):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi(self)

        # signals & slots
        self.btnOk.clicked.connect(self.accept)
        self.btnCancel.clicked.connect(self.reject)


if __name__ == '__main__':
    alta = UiAltaAnimales()
    del alta
