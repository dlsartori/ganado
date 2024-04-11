import sys
from PyQt6 import QtCore, QtWidgets
# https://stackoverflow.com/questions/52560496/getting-a-second-window-pass-a-variable-to-the-main-ui-and-close
from ui.IngresoAnimalesTabbed import Ui_dlgIngresoAnimales
from krnl_custom_types import DataTable, getRecords, dbRead
from datetime import datetime, timedelta


class UiAltaAnimales():
    def __init__(self, parent=None):

        app = QtWidgets.QApplication(sys.argv)
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

        # create lists of values for combo boxes
        self.lstTipoAlta = dbRead('tblAnimalesTiposDeAltaBaja',
                    'SELECT "Nombre Tipo De AltaBaja", "ID_Tipo De AltaBaja" FROM "Animales Tipos De AltaBaja"'
                    ' WHERE "AltaBaja" = "Alta" AND "GUI_Bitmask" & 1 ORDER BY "Nombre Tipo De AltaBaja"', 0).dataList
        self.lstTipoAlta.insert(0, ['', None])
        self.lstCategorias = dbRead('tblAnimalesCategorias',
                    'SELECT "Nombre Categoria", "ID_Categoria" FROM "Animales Categorias" WHERE'
                    ' "ID_Clase De Animal" = 1 AND "GUI_Bitmask" & 1 ORDER BY "Nombre Categoria"', 0).dataList
        self.lstCategorias.insert(0, ['', None])
        self.lstRazas = dbRead('tblAnimalesRazas',
                    'SELECT "Nombre Raza", "ID_Raza" FROM "Animales Razas" WHERE'
                    ' "ID_Clase De Animal" = 1 AND "GUI_Bitmask" & 1 ORDER BY "Nombre Raza"', 0).dataList
        self.lstRazas.insert(0, ['', None])
        self.lstEstablecimientos = dbRead('tblGeoEntidades',
                    'SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades" WHERE'
                    ' "ID_Nivel De Localizacion" = 40 AND "GUI_Bitmask" & 1 '
                    ' ORDER BY "Nombre Entidad"', 0).dataList
        self.lstEstablecimientos.insert(0, ['', None])
        self.lstPersonasTemp = dbRead('tblPersonas',
                    'SELECT "Apellidos", "Nombres / Razon Social", "ID_Persona" FROM "Personas" WHERE'
                    ' "GUI_Bitmask" & 1 ORDER BY "Persona"', 0).dataList
        self.lstPersonas = []
        for itm in self.lstPersonasTemp:
            if itm[2] == 1:
                continue
            self.lstPersonas.append([itm[0] + ' ' + itm[1], itm[2]])
        self.lstPersonas.insert(0, ['', None])
        self.lstMarcas = dbRead('tblAnimalesMarcas',
                    'SELECT "Nombre Marca", "ID_Marca" FROM "Animales Marcas" WHERE'
                    ' "ID_Clase De Animal" = 1 AND "GUI_Bitmask" & 1 ORDER BY "Nombre Marca"', 0).dataList
        self.lstMarcas.insert(0, ['', None])

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
        for item, intId in self.lstPersonas:
            self.cboDuenio.addItem(item, intId)
        for item, intId in self.lstMarcas:
            self.cboMarca.addItem(item, intId)
        self.cboLote.setEnabled(False)
        self.cboPotrero.setEnabled(False)
        self.cboLocacion.setEnabled(False)

        # populate controls
        self.datNacimiento.setDate(self.qdtFechaNac)
        self.datDestete.setDate(self.qdtFechaDestete)
        self.datServicio.setDate(self.qdtFechaServ)

        self.btnOk.clicked.connect(self.accept)
        self.btnCancel.clicked.connect(self.reject)

        # signals & slots
        self.btnOk.clicked.connect(self.accept)
        self.btnCancel.clicked.connect(self.reject)
        self.cboCategoria.currentIndexChanged.connect(self.categoriaChanged)
        self.cboEstablecimiento.currentIndexChanged.connect(self.establecimientoChanged)
        self.cboLote.currentIndexChanged.connect(self.loteChanged)

    def categoriaChanged(self):
        if self.cboCategoria.currentData() in [2, 3, 4]:
            self.chkPreniez.setVisible(True)
        else:
            self.chkPreniez.setVisible(False)

    def establecimientoChanged(self):
        self.currentFarm = self.cboEstablecimiento.currentData()
        self.lstLotes = []
        if self.currentFarm:
            self.currentFarmUID = dbRead('tblGeoEntidades',
                                         f'SELECT "UID_Objeto" FROM "Geo Entidades" WHERE "ID_GeoEntidad" = {self.currentFarm}',
                                         0).dataList[0][0]
            self.lstLotes = dbRead('tblGeoEntidades',
                                   f'SELECT "Nombre Entidad", "ID_GeoEntidad" FROM "Geo Entidades"'
                                   f' WHERE "ID_Nivel De Localizacion" = 50 AND "Containers" LIKE "%{self.currentFarmUID}%"'
                                   f' ORDER BY "Nombre Entidad"', 0).dataList

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
    alta = UiAltaAnimales()
    del alta
