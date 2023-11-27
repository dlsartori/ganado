#!/usr/bin/env python
# -*- coding: utf-8 -*-
from PyQt5.QtCore import Qt
from PyQt5.QtSql import QSqlTableModel, QSqlQuery
from PyQt5.QtWidgets import QApplication, QMessageBox, QPushButton
from PyQt5 import uic
import datetime
import sys
# https://stackoverflow.com/questions/32914488/global-variables-between-different-modules
# These objects from 'cfg' now have an additional reference here
# from cfg import getTblName as tbl
# from cfg import getFldName as fld
# from cfg import getMaxId as maxId
# from cfg import createDbConn as createDbConn

# https://www.riverbankcomputing.com/static/Docs/PyQt5/designer.html#PyQt5.uic.loadUiType
# https://stackoverflow.com/questions/22663716/working-with-pyqt-and-qt-designer-ui-files
DialogUI, DialogBase = uic.loadUiType("IngresoCaravanas.ui")


class DialogoInsertarCaravanas(DialogBase, DialogUI):
    """
    Dialog for insertion of new tags

    Single or batch insertion

    On accept, write to database
    """
    def __init__(self, parent=None):
        DialogBase.__init__(self, parent)
        self.setupUi(self)

            # Set attributes
        self.intDigits = 0

        # # Connect database
        # [self.db, self.dbGanadoConnCreated] = createDbConn("GanadoSQLite.db", "GanadoConnection")
        # print(tbl('tblAnimales'))
        # print(fld('tblAnimales', 'fldID'))

        # Populate combo boxes
        # If table name has whitespaces and sorting is needed, use QSqlQueryModel instead of QSqlTableModel:
        # modelTipo = QSqlQueryModel()
        # modelTipo.setQuery( QSqlQuery('SELECT * FROM [' + tbl('tblCaravanasTipos') + ']'
        #                               ' ORDER BY [' + fld('tblCaravanasTipos', 'fldTagType') + ']') )
        # print(modelTipo.selectStatement())
        # modelTipo = QSqlTableModel(self, self.db)
        # modelTipo.setTable('"' + tbl('tblCaravanasTipos') + '"')
        # modelTipo.select()
        # modelColores = QSqlTableModel(self, self.db)
        # modelColores.setTable('"' + tbl('tblColores') + '"')
        # modelColores.select()
        # modelColores.sort(1, Qt.AscendingOrder)
        # modelFormato = QSqlTableModel(self, self.db)
        # modelFormato.setTable('"' + tbl('tblCaravanasTecnologia') + '"')
        # modelFormato.select()
        # modelTecnologia = QSqlTableModel(self, self.db)
        # modelTecnologia.setTable('"' + tbl('tblCaravanasFormato') + '"')
        # modelTecnologia.select()
        # modelTecnologia.sort(1, Qt.AscendingOrder)
        # self.cboTipo.setModel(modelTipo)
        # self.cboTipo.setModelColumn(1)
        # self.cboColor.setModel(modelColores)
        # self.cboColor.setModelColumn(1)
        # self.cboFormato.setModel(modelFormato)
        # self.cboFormato.setModelColumn(1)
        # self.cboTecnologia.setModel(modelTecnologia)
        # self.cboTecnologia.setModelColumn(1)

        # Configure other items
        self.radIndividual.setChecked(True)
        self.onIdRadioToggled()
        self.progressBar.setHidden(True)

        # Connect signals to slots
        # self.btnOk.clicked.connect(self.commit)
        # self.btnCancel.clicked.connect(self.close)
        # self.radIndividual.toggled.connect(self.onIdRadioToggled)
        # self.chkCerosAdic.toggled.connect(self.onTagChanged)
        # self.spbDigitos.valueChanged.connect(self.onSpbDigitosChanged)
        # self.spbInicio.valueChanged.connect(self.onTagChanged)
        # self.spbFin.valueChanged.connect(self.onSpbFinChanged)
        # self.txtPrefijo.textChanged.connect(self.onTagChanged)
        # self.txtSufijo.textChanged.connect(self.onTagChanged)

    def onIdRadioToggled(self):
        """
        Show individual or batch insertion widgets

        :return: None
        """
        pass
        # if self.radIndividual.isChecked():
        #     # print('Individual')
        #     self.lblNombre.setHidden(False)
        #     self.lblNumeroInicio.setHidden(True)
        #     self.lblNumeroFin.setHidden(True)
        #     self.spbInicio.setHidden(True)
        #     self.spbFin.setHidden(True)
        #     self.txtId.setHidden(False)
        #     self.chkCerosAdic.setHidden(True)
        #     self.spbDigitos.setHidden(True)
        #     self.lblDigitos.setHidden(True)
        #     self.lblPrefijo.setHidden(True)
        #     self.txtPrefijo.setHidden(True)
        #     self.lblSufijo.setHidden(True)
        #     self.txtSufijo.setHidden(True)
        #     self.lblStatus.setHidden(True)
        # else:
        #     # print('Lote')
        #     self.lblNombre.setHidden(True)
        #     self.lblNumeroInicio.setHidden(False)
        #     self.lblNumeroFin.setHidden(False)
        #     self.spbInicio.setHidden(False)
        #     self.spbFin.setHidden(False)
        #     self.txtId.setHidden(True)
        #     self.chkCerosAdic.setHidden(False)
        #     self.spbDigitos.setHidden(False)
        #     self.lblDigitos.setHidden(False)
        #     self.lblPrefijo.setHidden(False)
        #     self.txtPrefijo.setHidden(False)
        #     self.lblSufijo.setHidden(False)
        #     self.txtSufijo.setHidden(False)
        #     self.lblStatus.setHidden(False)
        #     self.onTagChanged()

    def onSpbDigitosChanged(self):
        """
        If 'Fill with zeros' checkbox enabled, set intDigits attribute value according to 'digits' spinbox value

        :return: None
        """
        pass
        # self.intDigits = self.spbDigitos.value()
        # self.onTagChanged()

    def onSpbFinChanged(self):
        """
        If 'Fill with zeros' checkbox enabled, set 'digits' spinbox value according to 'End' spinbox number of digits

        :return: None
        """
        pass
        # self.intDigits = len(str(self.spbFin.value()))
        # self.spbDigitos.setMinimum(self.intDigits)
        # self.spbDigitos.setValue(self.intDigits)
        # self.onTagChanged()

    def onTagChanged(self):
        """
        Update status bar legend

        :return: None
        """
        pass
        # if self.chkCerosAdic.isChecked() == True:
        #     strNum = str(self.spbInicio.value()).rjust(self.intDigits, "0")
        # else:
        #     strNum = str(self.spbInicio.value())
        # strMuestra = self.txtPrefijo.text() + strNum  + self.txtSufijo.text()
        # self.lblStatus.setText(f'Muestra: {strMuestra}')

    # def commit(self):
    #     """
    #     Write values to database
    #
    #     :return: None
    #     """
    #     # Generate tag names list
    #     if self.radIndividual.isChecked():      # single tag
    #         lstNombres = [self.txtId.text()]
    #     else:                                   # batch
    #         if self.chkCerosAdic.isChecked():
    #             intDigits = self.intDigits
    #         else:
    #             intDigits = 0
    #         lstNombres = [self.txtPrefijo.text() + str(i).rjust(intDigits, "0") + self.txtSufijo.text()
    #                       for i in range(self.spbInicio.value(), self.spbFin.value() + 1)]
    #
    #     # 1) Validation:
    #     #    - Primary keys (unique and not null) → combo boxes have already selected values
    #     #    - Other not null values
    #     if self.radIndividual.isChecked():
    #         if self.txtId.text() == '':
    #             QMessageBox.warning(self, "Caravanas", "Se debe ingresar un identificador para la caravana")
    #             self.txtId.setFocus()
    #             return
    #     else:
    #         # - start and end numbers relation
    #         if self.spbInicio.value() > self.spbFin.value():
    #             QMessageBox.warning(self, "Caravanas", "El número de fin no pueden ser menor al de inicio")
    #             self.spbFin.setFocus()
    #             return
    #         if self.spbInicio.value() == self.spbFin.value():
    #             QMessageBox.warning(self, "Caravanas", "Los números de inicio y fin no pueden ser iguales")
    #             self.spbInicio.setFocus()
    #             return
    #     #    - Duplicate values
    #     strTblCaravanas = tbl('tblCaravanas')
    #     strFldNameCaravanas = fld('tblCaravanas', 'fldTagNumber')
    #     query = QSqlQuery(self.db)
    #     strValuesList = lstNombres.__repr__().replace("[", "(").replace("]", ")")
    #     lstDuplicates = []
    #     query.exec_(f"SELECT [{strFldNameCaravanas}] FROM [{strTblCaravanas}]"
    #                 f" WHERE [{strFldNameCaravanas}] IN {strValuesList}")
    #     if query.record().count() > 0:
    #         while query.next():
    #             lstDuplicates.append(query.value(0))
    #     intDuplicates = len(lstDuplicates)
    #     if intDuplicates == 1:
    #         QMessageBox.warning(self, "Valor duplicado", f"La caravana {lstDuplicates[0]} ya existe")
    #         return
    #
    #     if intDuplicates > 1:
    #         if intDuplicates == len(lstNombres):
    #             QMessageBox.warning(self, "Valores duplicados", f"Las caravanas ya existen")
    #             return
    #         else:
    #             box = QMessageBox()
    #             box.setIcon(QMessageBox.Warning)
    #             box.setWindowIcon(DialogBase.windowIcon(self))
    #             box.setWindowTitle('Valores duplicados')
    #             box.setText(f"Hay {intDuplicates} caravanas que ya existen.\n"
    #                         f"Presione Aceptar para agregar las demás caravanas.")
    #             box.setStandardButtons(QMessageBox.Yes | QMessageBox.No)
    #             buttonY = box.button(QMessageBox.Yes)
    #             buttonY.setText('Aceptar')
    #             buttonN = box.button(QMessageBox.No)
    #             buttonN.setText('Cancelar')
    #             box.exec_()
    #
    #             if box.clickedButton() == buttonN:
    #                 return
    #
    #     # 2) Data collection: define variables whith values for each field to be written
    #     #    - direct: taken from form widgets
    #     intTipo = self.cboTipo.model().index(self.cboTipo.currentIndex(),0).data()
    #     intColor = self.cboColor.model().index(self.cboColor.currentIndex(),0).data()
    #     intFormato = self.cboFormato.model().index(self.cboFormato.currentIndex(),0).data()
    #     intTecnologia = self.cboTecnologia.model().index(self.cboTecnologia.currentIndex(),0).data()
    #     strComentario = self.pteComentario.toPlainText()
    #     intStatus = 1   # Status = 1 → Not assigned
    #     intActividad = 1   # Actividad = 1 → Add tag
    #     intInventario = 0
    #     if self.radIndividual.isChecked():
    #         intPorLote = 0
    #     else:
    #         intPorLote = 1
    #     #    - calculated
    #     dateTime = datetime.datetime.now()
    #
    #     # 3) Input:
    #     if len(lstNombres) > 1:
    #         self.progressBar.setHidden(False)
    #     #    - Get next 'Caravanas' table ID (necessary for is compound PK)
    #     nextIdCaravana = maxId('tblCaravanas') + 1
    #     #    - Get next 'Data Caravanas Datos' table ID (optional)
    #     nextIDActividad = maxId('tblCaravanasRegistroDeActividades') + 1
    #
    #     i = 1
    #     query = QSqlQuery(self.db)
    #     query.exec_('BEGIN TRANSACTION;')
    #     for name in lstNombres:
    #         self.progressBar.setValue(int(i / len(lstNombres) * 100))
    #         strNombreCaravana = name
    #         strSQL1 = f" INSERT INTO [{tbl('tblCaravanas')}] (" \
    #                   f" [{fld('tblCaravanas', 'fldID')}]," \
    #                   f" [{fld('tblCaravanas', 'fldTagNumber')}]," \
    #                   f" [{fld('tblCaravanas', 'fldFK_Color')}]," \
    #                   f" [{fld('tblCaravanas', 'fldFK_TipoDeCaravana')}]," \
    #                   f" [{fld('tblCaravanas', 'fldFK_TecnologiaDeCaravana')}]," \
    #                   f" [{fld('tblCaravanas', 'fldFK_FormatoDeCaravana')}]," \
    #                   f" [{fld('tblCaravanas', 'fldTimeStamp')}]," \
    #                   f" [{fld('tblCaravanas', 'fldComment')}]" \
    #                   f" ) VALUES ({nextIdCaravana}, '{strNombreCaravana}', {intColor}, {intTipo}, {intTecnologia}, " \
    #                   f" {intFormato}, '{dateTime}', '{strComentario}');"
    #         strSQL2 = f" INSERT INTO [{tbl('tblCaravanasRegistroDeActividades')}] (" \
    #                   f" [{fld('tblCaravanasRegistroDeActividades', 'fldID')}]," \
    #                   f" [{fld('tblCaravanasRegistroDeActividades', 'fldFK_Caravana')}]," \
    #                   f" [{fld('tblCaravanasRegistroDeActividades', 'fldFK_NombreActividad')}]," \
    #                   f" [{fld('tblCaravanasRegistroDeActividades', 'fldTimeStamp')}]," \
    #                   f" [{fld('tblCaravanasRegistroDeActividades', 'fldFK_StatusCaravana')}]," \
    #                   f" [{fld('tblCaravanasRegistroDeActividades', 'fldInventory')}]," \
    #                   f" [{fld('tblCaravanasRegistroDeActividades', 'fldFlag')}]" \
    #                   f" ) VALUES ({nextIDActividad}, {nextIdCaravana}, '{intActividad}', '{dateTime}'," \
    #                   f" {intStatus}, {intInventario}, {intPorLote});"
    #         query.exec_(strSQL1)
    #         query.exec_(strSQL2)
    #         i += 1
    #         nextIdCaravana += 1
    #         # nextIDActividad += 1
    #     query.exec_('COMMIT;')
    #
    #     # 4) Cleanup:
    #     #    - Close connections and destroy objects
    #     if self.dbGanadoConnCreated:
    #         self.db.close()
    #         self.db.removeDatabase("GanadoSQLite.db")
    #     self.accept()


class spMessageBox(QMessageBox):
    def __init__(self, parent=None):
        QMessageBox.__init__(self, parent)
        self.setWindowTitle("Example")

        self.addButton(QPushButton("Aceptar"), QMessageBox.YesRole )
        # self.addButton(QPushButton("No"), QMessageBox.NoRole)
        self.addButton(QPushButton("Cancelar"), QMessageBox.RejectRole)
        ret = self.exec_()


# Usage
if __name__ == '__main__':
    app = QApplication(sys.argv)
    dialog = DialogoInsertarCaravanas()
    dialog.show()
    sys.exit(app.exec_())
