from PyQt6.QtWidgets import QApplication, QDialog, QMainWindow
from PyQt6 import QtCore
from PyQt6.QtCore import Qt
import sys
# https://stackoverflow.com/questions/35950050/how-to-import-python-file-located-in-same-subdirectory-in-a-pycharm-project
from ui.Test import Ui_MainWindow
from custom_types import DataTable, getRecords, setRecord


class MyDialog(QMainWindow, Ui_MainWindow):
    def __init__(self, parent=None):
        super().__init__()
        self.setupUi(self)
        self.btnSalir.clicked.connect(self.close)


class TableModel(QtCore.QAbstractTableModel):
    def __init__(self, data):
        super(TableModel, self).__init__()
        self._data = data

    def data(self, index, role):
        if role == Qt.ItemDataRole.DisplayRole:
            # See below for the nested-list data structure.
            # .row() indexes into the outer list,
            # .column() indexes into the sub-list
            return self._data[index.row()][index.column()]

    def rowCount(self, index):
        # The length of the outer list.
        return len(self._data)

    def columnCount(self, index):
        # The following takes the first sub-list, and returns
        # the length (only works if all rows are an equal length)
        return len(self._data[0])

def bovine_test():
    data = [[11, 12, 13, 14, 15],
            [21, 22, 23, 24, 25],
            [31, 32, 33, 34, 35]]

    tbl1 = DataTable('tblAnimales')
    tblAnimalesClases = getRecords('tblAnimalesClases', '', '', None, 'fldID', 'fldAnimalClass')
    tblCategoria = getRecords('tblAnimalesCategorias', '', '', None, 'fldID',
                              'fldFK_ClaseDeAnimal', 'fldName', 'fldMF', 'fldFlagCastrado')
    tblActividadesNombresBov = getRecords('tblAnimalesActividadesNombres', '', '', None, 'fldID',
                              'fldFK_ClaseDeAnimal', 'fldName', 'fldNivelRequerido', 'fldFlag', 'fldFlagPA')

    print(f'Key Field Names for table {tblCategoria.tblName}: {tblCategoria.fldNames}')
    print(f'DB Field Names  for table {tblCategoria.tblName}: {tblCategoria.dbFldNames}')
    print(f'Field Name Map  for table {tblCategoria.tblName}: {tblCategoria.fldMap()}')
    print(f'Unpacking item {tblCategoria.tblName}(0): {tblCategoria.unpackItem(0)}')
    print(f'Iterating over table {tblCategoria.tblName}: ')
    print(f'{tblCategoria.fldNames}')
    for i in range(tblCategoria.dataLen):
        print(f'{tblCategoria.dataList[i]}')

    # setRecord('tblAnimalesCategorias', **tblCategoria.unpackItem(0))
    # tblCategoria.setRecords()
    app = QApplication(sys.argv)
    dialog = MyDialog()
    model = TableModel(tblActividadesNombresBov.dataList)
    dialog.tableViewAnimales.setModel(model)
    dialog.show()
    # dialog.activateWindow()
    # dialog.raise_()
    app.exec()
    return False

if __name__ == '__main__':
    bovine_test()
