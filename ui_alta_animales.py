import sys
from PyQt6 import QtCore, QtWidgets
# https://stackoverflow.com/questions/52560496/getting-a-second-window-pass-a-variable-to-the-main-ui-and-close

from ui.IngresoAnimales import Ui_dlgIngresoAnimales
from ui.IngresoAnimalesAdic import Ui_dlgIngresoAnimalesAdic
from ui.IngresoAnimalesCaravanas import Ui_dlgIngresoAnimalesCaravanas
from custom_types import DataTable, getRecords, dbRead


class UiAltaAnimales():
    def __init__(self, parent=None):

        app = QtWidgets.QApplication(sys.argv)
        dlgAlta = DialogAltaAnimales()
        if dlgAlta.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            print('Accepted')
            dlgAlta.intCategoria = dlgAlta.cboCategoria.currentData()
            print('intCategoria: ' + str(dlgAlta.intCategoria))
        else:
            print('Rejected')


class DialogAltaAnimales(QtWidgets.QDialog, Ui_dlgIngresoAnimales):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi(self)

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

        # lstAnimalesCategorias = getRecords('tblAnimalesCategorias', '', '', None, 'fldID', 'fldName').dataList
        self.lstAnimalesCategorias = dbRead('tblAnimalesCategorias',
                    'SELECT "Nombre Categoria", "ID_Categoria" FROM "Animales Categorias" WHERE'
                    ' "ID_Clase De Animal" = 1 ORDER BY "Nombre Categoria"', 0).dataList
        for item, intId in self.lstAnimalesCategorias:
            self.cboCategoria.addItem(item, str(intId))

        self.btnAdic.clicked.connect(self.dialogAdic)
        self.btnCaravanas.clicked.connect(self.dialogCaravanas)
        self.btnOk.clicked.connect(self.accept)
        self.btnCancel.clicked.connect(self.reject)

    def dialogAdic(self):   # show additional information dialog
        dlgAdic = DialogAltaAnimalesAdic()
        dlgAdic.dspPeso.setValue(self.dblPeso)
        if dlgAdic.exec() == QtWidgets.QDialog.DialogCode.Accepted:
            self.dblPeso = dlgAdic.dspPeso.value()
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

        self.btnOk.clicked.connect(self.accept)
        self.btnCancel.clicked.connect(self.reject)


class DialogAltaAnimalesCaravanas(QtWidgets.QDialog, Ui_dlgIngresoAnimalesCaravanas):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi(self)

        self.btnOk.clicked.connect(self.accept)
        self.btnCancel.clicked.connect(self.reject)


if __name__ == '__main__':
    alta = UiAltaAnimales()
    del alta
