#!/usr/bin/env python
# -*- coding: utf-8 -*-
from PyQt5.QtCore import Qt, QDate
from PyQt5.QtSql import QSqlDatabase, QSqlTableModel, QSqlQuery, QSqlQueryModel
from PyQt5.QtWidgets import QApplication, QMessageBox, QPushButton, QLineEdit
from PyQt5 import uic, QtCore
import datetime
from datetime import timedelta
# from python-dateutil.relativedelta import relativedelta
import sys
# https://stackoverflow.com/questions/32914488/global-variables-between-different-modules
# import cfg  # bind objects referenced by variables in cfg to like-named variables in this module
# These objects from 'cfg' now have an additional reference here
# from cfg import getTblName as tbl
# from cfg import getFldName as fld
# from cfg import getMaxId as maxID
# from cfg import createDbConn as createDbConn
from comboTabla import ComboTabla

# https://www.riverbankcomputing.com/static/Docs/PyQt5/designer.html#PyQt5.uic.loadUiType
# https://stackoverflow.com/questions/22663716/working-with-pyqt-and-qt-designer-ui-files
DialogUI, DialogBase = uic.loadUiType("IngresoAnimalesAdic.ui")


class DialogoAnimalesAdic(DialogBase, DialogUI):
    """
    Input dialog for additional animal data

    On accept, dialog is hidden and object's attributes are exposed to be queried, until object is destroyed (see usage section)
    """
    def __init__(self, intLote=None, intPotrero=None, intLocacion=None,
                 dblPeso=None, qdtFechaNac=None, qdtFechaDestete=None, qdtFechaServ=None,
                 blnTratamientos=None, blnDieta=None, parent=None):
        DialogBase.__init__(self, parent)
        self.setupUi(self)

        # Set attributes
        self.intLote = intLote
        self.intPotrero = intPotrero
        self.intLocacion = intLocacion
        self.dblPeso = dblPeso
        self.qdtFechaNac = qdtFechaNac          # as QDate
        self.qdtFechaDestete = qdtFechaDestete if qdtFechaDestete else self.qdtFechaNac.addDays(60)  # as QDate
        self.qdtFechaServ = qdtFechaServ if qdtFechaServ else self.qdtFechaNac.addDays(180)        # as QDate
        self.blnTratamientos = blnTratamientos
        self.blnDieta = blnDieta
        self.lastKey = -1

        # Connect database
        # [self.db, self.dbGanadoConnCreated] = createDbConn("GanadoSQLite.db", "GanadoConnection")

        # Signals - Slots
        # self.datNacimiento.dateChanged.connect(self.onFechaNacChanged)
        # self.dspEdad.valueChanged.connect(self.onEdadChanged)
        # self.btnOk.clicked.connect(self.commit)
        # self.btnCancel.clicked.connect(self.close)

        # # Fill Combo Boxes
        # modelLote = QSqlQueryModel()
        # strSQL = f"SELECT [{fld('tblGeoEstablecimientosLotes', 'fldID')}]," \
        #          f" [{fld('tblGeoEstablecimientosLotes', 'fldName')}]"\
        #          f" FROM [{tbl('tblGeoEstablecimientosLotes')}]"\
        #          f" UNION SELECT -1 AS [{fld('tblGeoEstablecimientosLotes', 'fldID')}]," \
        #          f" \'----\' AS [{fld('tblGeoEstablecimientosLotes', 'fldName')}]" \
        #          f" ORDER BY [{fld('tblGeoEstablecimientosLotes', 'fldName')}]"
        # modelLote.setQuery(QSqlQuery(strSQL, self.db))
        # modelPotrero = QSqlQueryModel()
        # strSQL = f"SELECT [{fld('tblGeoEstablecimientosPotreros', 'fldID')}]," \
        #          f" [{fld('tblGeoEstablecimientosPotreros', 'fldName')}]"\
        #          f" FROM [{tbl('tblGeoEstablecimientosPotreros')}]"\
        #          f" UNION SELECT -1 AS [{fld('tblGeoEstablecimientosPotreros', 'fldID')}]," \
        #          f" \'----\' AS [{fld('tblGeoEstablecimientosPotreros', 'fldName')}]" \
        #          f" ORDER BY [{fld('tblGeoEstablecimientosPotreros', 'fldName')}]"
        # modelPotrero.setQuery(QSqlQuery(strSQL, self.db))
        # modelLocacion = QSqlQueryModel()
        # strSQL = f"SELECT [{fld('tblGeoEstablecimientosLocaciones', 'fldID')}]," \
        #          f" [{fld('tblGeoEstablecimientosLocaciones', 'fldName')}]"\
        #          f" FROM [{tbl('tblGeoEstablecimientosLocaciones')}]"\
        #          f" UNION SELECT -1 AS [{fld('tblGeoEstablecimientosLocaciones', 'fldID')}]," \
        #          f" \'----\' AS [{fld('tblGeoEstablecimientosLocaciones', 'fldName')}]" \
        #          f" ORDER BY [{fld('tblGeoEstablecimientosLocaciones', 'fldName')}]"
        # modelLocacion.setQuery(QSqlQuery(strSQL, self.db))
        # self.cboLote.setModel(modelLote)
        # self.cboLote.setModelColumn(1)
        # self.cboPotrero.setModel(modelPotrero)
        # self.cboPotrero.setModelColumn(1)
        # self.cboLocacion.setModel(modelLocacion)
        # self.cboLocacion.setModelColumn(1)
        # # Select every combo box value according to IDs passed as argument
        # if self.intLote and self.cboLote.model().rowCount() > 1:
        #     idxLote = self.cboLote.model().match(self.cboLote.model().index(0, 0),
        #                                          QtCore.Qt.DisplayRole, self.intLote, 1)[0]
        #     strLote = idxLote.siblingAtColumn(1).data()
        #     intPosLote = self.cboLote.findData(strLote, Qt.DisplayRole)
        #     self.cboLote.setCurrentIndex(intPosLote)
        # if self.intPotrero and self.cboPotrero.model().rowCount() > 1:
        #     idxPotrero = self.cboPotrero.model().match(self.cboPotrero.model().index(0, 0),
        #                                                QtCore.Qt.DisplayRole, self.intPotrero, 1)[0]
        #     strPotrero = idxPotrero.siblingAtColumn(1).data()
        #     intPosPotrero = self.cboPotrero.findData(strPotrero, Qt.DisplayRole)
        #     self.cboPotrero.setCurrentIndex(intPosPotrero)
        # if self.intLocacion and self.cboLocacion.model().rowCount() > 1:
        #     idxLocacion = self.cboLocacion.model().match(self.cboLocacion.model().index(0, 0),
        #                                                QtCore.Qt.DisplayRole, self.intLocacion, 1)[0]
        #     strLocacion = idxLocacion.siblingAtColumn(1).data()
        #     intPosLocacion = self.cboLocacion.findData(strLocacion, Qt.DisplayRole)
        #     self.cboLocacion.setCurrentIndex(intPosLocacion)
        #
        # # Fill other Widgets' values
        # if self.dblPeso:
        #     self.dspPeso.setValue(self.dblPeso)
        # if self.qdtFechaNac:
        #     self.datNacimiento.setDate(self.qdtFechaNac)
        #     self.onFechaNacChanged()
        # if self.qdtFechaDestete:
        #     self.datDestete.setDate(self.qdtFechaDestete)
        # else:
        #     self.datDestete.findChild(QLineEdit).setText('')
        # if self.qdtFechaServ:
        #     self.datServicio.setDate(self.qdtFechaServ)
        # else:
        #     self.datServicio.findChild(QLineEdit).setText('')
        # if self.blnTratamientos:
        #     self.chkTratamientos.setChecked(True)
        # if self.blnDieta:
        #     self.chkDieta.setChecked(True)


    def onFechaNacChanged(self):
        """
        Change age value according to date of birth input

        :return: None
        """
        pass
        # if self.datNacimiento.date() > datetime.date.today():
        #     self.datNacimiento.setDate(datetime.date.today())
        #     self.datNacimiento.setFocus()
        # difYears = relativedelta(datetime.date.today(), self.datNacimiento.date().toPyDate())
        # self.dspEdad.valueChanged.disconnect()
        # self.dspEdad.setValue(difYears.years + difYears.months / 12 + difYears.days / 365)
        # self.dspEdad.valueChanged.connect(self.onEdadChanged)

    def onEdadChanged(self):
        """
        Change date of birth value according to age input

        :return: None
        """
        pass
        # d = datetime.date.today() - timedelta(days=self.dspEdad.value() * 365)
        # self.datNacimiento.dateChanged.disconnect()
        # self.datNacimiento.setDate(d)
        # self.datNacimiento.dateChanged.connect(self.onFechaNacChanged)

    # def commit(self):
    #     """
    #     Set attributes and hide the dialog
    #
    #     :return: None
    #     """
    #     # 1) Validation
    #     #    - Combo Boxes: select a value from the list
    #     lstCboNotNull = ['cboLote']
    #     for wdg in self.findChildren(ComboTabla):
    #         if wdg.findText(wdg.currentText()) == 0 and wdg.objectName() in lstCboNotNull:
    #             QMessageBox.warning(self, "Valor requerido",
    #                                 "Se debe elegir un valor de la lista")
    #             wdg.setFocus()
    #             return
    #     #    - Not null values
    #     #    - Duplicate values
    #
    #     # 2) Data collection: set attributes to be exposed once the dialog is accepted and hidden
    #     self.qdtFechaNac = self.datNacimiento.date()        # as QDate
    #     self.qdtFechaDestete = self.datDestete.date()       # as QDate
    #     self.qdtFechaServ = self.datServicio.date()         # as QDate
    #     self.dblPeso = self.dspPeso.value()
    #     self.intLote = self.cboLote.model().index(self.cboLote.currentIndex(), 0).data()
    #     self.intPotrero = self.cboPotrero.model().index(self.cboPotrero.currentIndex(), 0).data()
    #     self.intLocacion = self.cboLocacion.model().index(self.cboLocacion.currentIndex(), 0).data()
    #     self.blnTratamientos = True if self.chkTratamientos.isChecked() else False
    #     self.blnDieta = True if self.chkDieta.isChecked() else False
    #
    #     # 3) Cleanup:
    #     #    - Close connections and destroy objects
    #     if self.dbGanadoConnCreated:
    #         self.db.close()
    #         QSqlDatabase.removeDatabase("GanadoSQLite.db")
    #     self.accept()


# Usage
if __name__ == '__main__':
    app = QApplication(sys.argv)

    # dialog = DialogoAnimalesAdic()
    dialog = DialogoAnimalesAdic(intLote=1, intPotrero=1, intLocacion=None, dblPeso=500.0,
            qdtFechaNac=QDate(2020, 6, 18), qdtFechaDestete=QDate(2020, 9, 20), qdtFechaServ=QDate(2021, 4, 22))

    if dialog.exec_():
        print(dialog.intLote, dialog.intPotrero, dialog.intLocacion, dialog.dblPeso,
              dialog.qdtFechaNac, dialog.qdtFechaDestete, dialog.qdtFechaServ,
              dialog.blnTratamientos, dialog.blnDieta)
        del dialog
