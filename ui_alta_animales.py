import datetime
import sys
from PyQt6 import QtCore, QtWidgets
# https://stackoverflow.com/questions/52560496/getting-a-second-window-pass-a-variable-to-the-main-ui-and-close

from ui.IngresoAnimales import Ui_dlgIngresoAnimales
from ui.IngresoAnimalesAdic import Ui_dlgIngresoAnimalesAdic
from ui.IngresoAnimalesCaravanas import Ui_dlgIngresoAnimalesCaravanas
from krnl_custom_types import DataTable, getRecords, dbRead
from datetime import datetime, timedelta


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
            dlgAlta.blnPregn = dlgAlta.chkPreniez.isChecked()
            dlgAlta.strComentario = dlgAlta.pteComentario.toPlainText()
            # TEMP dlgAlta
            print('Alta Animales:')
            print('    intTipoEntrada: ' + str(dlgAlta.intTipoEntrada))
            print('    intCategoria: ' + str(dlgAlta.intCategoria))
            print('    intRaza: ' + str(dlgAlta.intRaza))
            print('    intEstablecimiento: ' + str(dlgAlta.intEstablecimiento))
            print('    intDuenio: ' + str(dlgAlta.intDuenio))
            print('    intMarca: ' + str(dlgAlta.intMarca))
            print('    blnPregn: ' + str(dlgAlta.blnPregn))
            print('    strComentario: ' + str(dlgAlta.strComentario))
            # TEMP dlgAdic
            print('Adicional:')
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
        self.intDuenio = 1  # YA
        self.intMarca = 1  # YA
        self.blnPregn = True  # YA
        self.strComentario = '' # YA
        self.lstIdCaravanas = []
        self.qdtFechaNac = QtCore.QDate.currentDate().addMonths(-24)
        self.qdtFechaDestete = QtCore.QDate.currentDate().addMonths(-18)
        self.qdtFechaServ = QtCore.QDate.currentDate().addMonths(-3)
        self.dblPeso = 150.0
        self.intLote = 1
        self.intPotrero = 1
        self.intLocacion = 1
        self.blnTratamientos = False
        self.blnDieta = False

        # create lists of values for combo boxes
        self.lstTipoAlta = dbRead('tblAnimalesTiposDeAltaBaja',
                    'SELECT "Nombre Tipo De AltaBaja", "ID_Tipo De AltaBaja" FROM "Animales Tipos De AltaBaja"'
                    ' WHERE "AltaBaja" = "Alta" ORDER BY "Nombre Tipo De AltaBaja"', 0).dataList
        self.lstCategorias = dbRead('tblAnimalesCategorias',
                    'SELECT "Nombre Categoria", "ID_Categoria" FROM "Animales Categorias" WHERE'
                    ' "ID_Clase De Animal" = 1 ORDER BY "Nombre Categoria"', 0).dataList
        self.lstRazas = getRecords('tblAnimalesRazas', '', '', None, 'fldName', 'fldID').dataList
        self.lstEstablecimientos = dbRead('tblGeoEntidades',
                    'SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades" WHERE'
                    ' "ID_Nivel De Localizacion" = 40'
                    ' ORDER BY "Nombre Entidad"', 0).dataList   #  AND NOT "ID_GeoEntidad" = 2
        self.lstPersonasTemp = getRecords('tblPersonas', '', '', None, 'fldLastName', 'fldName', 'fldID').dataList
        self.lstPersonas = []
        for itm in self.lstPersonasTemp:
            if itm[2] == 1:
                continue
            self.lstPersonas.append([itm[0] + ' ' + itm[1], itm[2]])
        self.lstMarcas = getRecords('tblAnimalesMarcas', '', '', None, 'fldName', 'fldID').dataList

        # populate combo boxes
        for item, intId in self.lstTipoAlta:
            self.cboTipoDeEntrada.addItem(item, str(intId))
        for item, intId in self.lstCategorias:
            self.cboCategoria.addItem(item, str(intId))
        for item, intId in self.lstRazas:
            self.cboRaza.addItem(item, str(intId))
        for item, intId in self.lstEstablecimientos:
            if intId == 2:  # Establecimiento nulo
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
        # populate controls
        dlgAdic.datNacimiento.setDate(self.qdtFechaNac)
        dlgAdic.datDestete.setDate(self.qdtFechaDestete)
        dlgAdic.datServicio.setDate(self.qdtFechaServ)
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
                                   f' WHERE "ID_Nivel De Localizacion" = 50 AND "Containers" LIKE "%{self.currentFarmUID}%"'
                                   f' ORDER BY "Nombre Entidad"', 0).dataList
            dlgAdic.cboLote.setEnabled(True)
            for item, intId in self.lstLotes:
                dlgAdic.cboLote.addItem(item, str(intId))
        dlgAdic.dspPeso.setValue(self.dblPeso)

        # additional signals & slots

        # run dialog
        if dlgAdic.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            # set result variables
            self.qdtFechaNac = dlgAdic.datNacimiento.date()
            self.qdtFechaDestete = dlgAdic.datDestete.date()
            self.qdtFechaServ = dlgAdic.datServicio.date()
            self.dblPeso = dlgAdic.dspPeso.value()
            self.intLote = dlgAdic.cboLote.currentData()
            self.intPotrero = dlgAdic.cboPotrero.currentData()
            self.intLocacion = dlgAdic.cboLocacion.currentData()
            self.blnTratamientos = dlgAdic.chkTratamientos.isChecked()
            self.blnDieta = dlgAdic.chkDieta.isChecked()
            print('Accepted')
        else:
            print('Rejected')

    def dialogCaravanas(self):  # show tag selection dialog
        dlgCaravanas = DialogAltaAnimalesCaravanas()

        # additional signals & slots

        # run dialog
        if dlgCaravanas.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            print('Accepted')
            self.lstIdCaravanas = []
        else:
            print('Rejected')


class DialogAltaAnimalesAdic(QtWidgets.QDialog, Ui_dlgIngresoAnimalesAdic):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi(self)

        self.currentField = None
        self.currentPotrero = None
        self.currentLocacion = None
        self.cboPotrero.setEnabled(False)
        self.cboLocacion.setEnabled(False)

        # signals & slots
        self.btnOk.clicked.connect(self.accept)
        self.btnCancel.clicked.connect(self.reject)
        self.cboLote.currentIndexChanged.connect(self.loteChanged)  # si hay un valor en cboLote, se ejecuta al cargar


    def loteChanged(self):
        # Si el lote elegido tiene potreros, habilitar el combo cboPotrero y cargar los items
        # Si el lote elegido tiene locaciones, habilitar el combo cboLocacion y cargar los items
        self.currentField = self.cboLote.currentData()
        if self.currentField:
            self.cboPotrero.clear()
            self.currentFieldUID = dbRead('tblGeoEntidades',
                                         f'SELECT "UID_Objeto" FROM "Geo Entidades" WHERE "ID_GeoEntidad" = {self.currentField}',
                                         0).dataList[0][0]
            # create lists of values for combo boxes
            self.lstPotreros = dbRead('tblGeoEntidades',
                                   f'SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades"'
                                   f' WHERE "ID_Nivel De Localizacion" = 60 AND "Containers" LIKE "%{self.currentFieldUID}%"'
                                   f' ORDER BY "Nombre Entidad"', 0).dataList
            self.lstLocaciones = dbRead('tblGeoEntidades',
                                   f'SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades"'
                                   f' WHERE "ID_Nivel De Localizacion" = 70 AND "Containers" LIKE "%{self.currentFieldUID}%"'
                                   f' ORDER BY "Nombre Entidad"', 0).dataList
            # populate combo boxes
            if self.lstPotreros == []:
                self.cboPotrero.clear()
                self.cboPotrero.setEnabled(False)
            else:
                for item, intId in self.lstPotreros:
                    self.cboPotrero.addItem(item, str(intId))
                self.cboPotrero.setEnabled(True)
            if self.lstLocaciones == []:
                self.cboLocacion.clear()
                self.cboLocacion.setEnabled(False)
            else:
                for item, intId in self.lstLocaciones:
                    self.cboLocacion.addItem(item, str(intId))
                self.cboLocacion.setEnabled(True)



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
