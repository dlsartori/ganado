#!/usr/bin/env python
# -*- coding: utf-8 -*-
from PyQt5.QtCore import QObject, QModelIndex
from PyQt5.QtSql import QSqlDatabase, QSqlQuery, QSqlQueryModel
from PyQt5.QtWidgets import QApplication, QMessageBox, QPushButton, QLabel, QFileDialog
from PyQt5 import QtCore
import datetime
import sys
sys.path.append("")
from krnl_cfg import createDbConn as createDbConn


# https://www.riverbankcomputing.com/static/Docs/PyQt5/designer.html#PyQt5.uic.loadUiType
# https://stackoverflow.com/questions/22663716/working-with-pyqt-and-qt-designer-ui-files
DialogUIDbDocMain, DialogBaseDbDocMain = uic.loadUiType("ui/DbDocMain.ui")
DialogUIDatosCampos, DialogBaseDatosCampos = uic.loadUiType("ui/DatosCampos.ui")


class DbDoc(DialogBaseDbDocMain, DialogUIDbDocMain):
    def __init__(self, parent=None):
        DialogBaseDbDocMain.__init__(self, parent)
        self.setupUi(self)

        self.intDigits = 0
        self.syncState = False

        [self.db, self.dbGanadoConnCreated] = createDbConn("GanadoSQLite.db", "GanadoConnection")
        self.qry = QSqlQuery(self.db)
        # if not obj.qry.exec_(F"PRAGMA journal_mode=WAL"):
        #     print(obj.qry.lastError().text())
        #     sys.exit(-1)
        # if not obj.qry.exec_(F"ATTACH '{obj.devdb}' AS 'configdb'"):
        #     print(obj.qry.lastError().text())
        #     sys.exit(-1)
        if not self.qry.exec_(F"SELECT MAX([Numero Version]) from [Sys Versiones DB]"):
            print(self.qry.lastError().text())
            self.dbVersion = ''
            sys.exit(-1)
        else:
            self.qry.next()
            self.dbVersion = self.qry.value(0)
        self.modelMain = QSqlQueryModel()
        self.modelTablas = QSqlQueryModel()
        self.modelCampos = QSqlQueryModel()
        self.modelValores = QSqlQueryModel()
        self.lblStatus = QLabel('')
        self.lblStatus.setStyleSheet("color: red;")
        self.btnVer = QPushButton('&Ver')
        self.btnVer.setStyleSheet("padding: 2px 2px 2px 2px;")
        # obj.val = QWidget()
        # obj.val.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.btnActualizar = QPushButton('&Actualizar')
        self.statusbar.addWidget(self.lblStatus)
        self.statusbar.addWidget(self.btnVer)
        # obj.statusbar.addWidget(obj.val)
        self.statusbar.addWidget(self.btnActualizar)
        self.btnActualizar.setStyleSheet("padding: 2px 2px 2px 2px;")
        # obj.btnSincTablas1.setStyleSheet("padding: 2px 2px 2px 2px;")
        # obj.btnSincTablas2.setStyleSheet("padding: 2px 2px 2px 2px;")
        # obj.btnSincCampos1.setStyleSheet("padding: 2px 2px 2px 2px;")
        # obj.btnSincCampos2.setStyleSheet("padding: 2px 2px 2px 2px;")
        # obj.btnRenamedTable.setStyleSheet("padding: 2px 2px 2px 2px;")
        # obj.btnRenamedField.setStyleSheet("padding: 2px 2px 2px 2px;")
        self.strTableViewStyleSheet = "QHeaderView::section {background-color:rgb(200, 200, 200); border-style: none;}"
        self.refreshAll()
        self.tableViewTablas.selectionModel().selectionChanged.connect(self.displayTableInfo)
        self.tableViewDb.doubleClicked.connect(self.editDB)
        self.tableViewCampos.doubleClicked.connect(self.editCampo)
        self.tableViewTablas.doubleClicked.connect(self.editTabla)
        self.listViewTablasWork.doubleClicked.connect(self.tablasWorkSync)
        self.listViewTablasDoc.doubleClicked.connect(self.tablasDocSync)
        self.listViewCamposWork.doubleClicked.connect(self.camposWorkSync)
        self.listViewCamposDoc.doubleClicked.connect(self.camposDocSync)
        self.listViewTablasSinKeyName.doubleClicked.connect(self.addTableKeyName)
        self.listViewCamposSinKeyName.doubleClicked.connect(self.addFieldKeyName)
        self.tabWidget.currentChanged.connect(self.btnVerVisibility)
        self.btnVer.clicked.connect(self.btnVerClick)
        self.btnSyncAll.clicked.connect(self.btnSyncAllClick)
        self.btnActualizar.clicked.connect(self.refreshAll)
        self.btnSincTablas1.clicked.connect(self.btnSincTablas1Click)
        self.btnSincTablas2.clicked.connect(self.btnSincTablas2Click)
        self.btnSincCampos1.clicked.connect(self.btnSincCampos1Click)
        self.btnSincCampos2.clicked.connect(self.btnSincCampos2Click)
        self.btnRenamedTable.clicked.connect(self.btnRenamedTableClick)
        self.btnRenamedField.clicked.connect(self.btnRenamedFieldClick)
        self.btnFieldsToCSV.clicked.connect(self.fieldsToCSV)
        self.btnTablesToCSV.clicked.connect(self.tablesToCSV)
        self.refreshSyncState()
        if self.listViewTablasWork.model().rowCount() > 0:
            self.listViewTablasWork.setCurrentIndex(self.listViewTablasWork.model().index(0,0))
        if self.listViewTablasDoc.model().rowCount() > 0:
            self.listViewTablasDoc.setCurrentIndex(self.listViewTablasDoc.model().index(0,0))
        if self.listViewCamposWork.model().rowCount() > 0:
            self.listViewCamposWork.setCurrentIndex(self.listViewCamposWork.model().index(0,0))
        if self.listViewCamposDoc.model().rowCount() > 0:
            self.listViewCamposDoc.setCurrentIndex(self.listViewCamposDoc.model().index(0,0))
        self.btnVerVisibility()
        self.renameButtonsVisibility()

    def refreshMain(self):
        strSqlGeneral = f"SELECT _sys_Tables.ID_Table AS 'ID Tabla',  _sys_Fields.ID_Field AS 'ID Campo', " \
                        f" _sys_Tables.Table_Name AS 'Nombre de Tabla',  _sys_Tables.Table_Key_Name AS 'Key Name Tabla'," \
                        f" _sys_Tables.Comments AS 'Comentarios Tabla', _sys_Fields.Field_Name as 'Nombre de Campo'," \
                        f" _sys_Fields.Field_Key_Name as 'Key Name Campo'," \
                        f" _sys_Fields.Comments as 'Comentarios Campo'" \
                        f"FROM _sys_Tables INNER JOIN _sys_Fields ON _sys_Tables.ID_Table = _sys_Fields.ID_Table" \
                        f" ORDER BY _sys_Tables.Table_Name"
        self.modelMain.setQuery(strSqlGeneral, self.db)
        self.tableViewDb.setModel(self.modelMain)
        while self.tableViewDb.model().canFetchMore():
            self.tableViewDb.model().fetchMore()
        self.tableViewDb.verticalHeader().setDefaultSectionSize(14)
        self.tableViewDb.horizontalHeader().setVisible(True)
        self.tableViewDb.horizontalHeader().setFixedHeight(14)
        self.tableViewDb.horizontalHeader().setDefaultAlignment(QtCore.Qt.AlignTop)
        self.tableViewDb.horizontalHeader().setStyleSheet(self.strTableViewStyleSheet)
        self.tableViewDb.resizeColumnsToContents()
        self.tableViewDb.setColumnHidden(0, True)
        self.tableViewDb.setColumnHidden(1, True)
        self.tableViewDb.setColumnHidden(4, True)

    def refreshTables(self):
        strSqlTablas = f"SELECT ID_Table AS 'ID', Table_Name as 'Nombre de Tabla', " \
                       f" Table_Key_Name as 'Key Name', Comments AS 'Comentarios'" \
                       f" FROM _sys_Tables" \
                       f" ORDER BY Table_Name"
        self.modelTablas.setQuery(strSqlTablas, self.db)
        self.tableViewTablas.setModel(self.modelTablas)
        while self.tableViewTablas.model().canFetchMore():
            self.tableViewTablas.model().fetchMore()
        self.tableViewTablas.verticalHeader().setDefaultSectionSize(14)
        self.tableViewTablas.horizontalHeader().setVisible(True)
        self.tableViewTablas.horizontalHeader().setFixedHeight(14)
        self.tableViewTablas.horizontalHeader().setDefaultAlignment(QtCore.Qt.AlignTop)
        self.tableViewTablas.horizontalHeader().setStyleSheet(self.strTableViewStyleSheet)
        self.tableViewTablas.resizeColumnsToContents()
        self.tableViewTablas.setColumnHidden(0, True)
        self.tableViewTablas.selectRow(0)

    def refreshFields(self):
        intTableId = self.tableViewTablas.selectionModel().selection().indexes()[0].data()
        strSqlCampos = f"SELECT ID_Table, ID_Field AS 'ID', Field_Name as 'Nombre de Campo', " \
                       f" Field_Key_Name as 'Key Name', Comments AS 'Comentarios'" \
                       f" FROM _sys_Fields" \
                       f" WHERE ID_Table = {intTableId}" \
                       f" ORDER BY ID_Field"
        self.modelCampos.setQuery(strSqlCampos, self.db)
        self.tableViewCampos.setModel(self.modelCampos)
        while self.tableViewCampos.model().canFetchMore():
            self.tableViewCampos.model().fetchMore()
        self.tableViewCampos.verticalHeader().setDefaultSectionSize(14)
        self.tableViewCampos.horizontalHeader().setVisible(True)
        self.tableViewCampos.horizontalHeader().setFixedHeight(14)
        self.tableViewCampos.horizontalHeader().setDefaultAlignment(QtCore.Qt.AlignTop)
        self.tableViewCampos.horizontalHeader().setStyleSheet(self.strTableViewStyleSheet)
        self.tableViewCampos.resizeColumnsToContents()
        self.tableViewCampos.setColumnHidden(0, True)
        self.tableViewCampos.setColumnHidden(1, True)
        self.tableViewCampos.selectRow(0)

    def refreshValues(self):
        strTableName = self.tableViewTablas.selectionModel().selection().indexes()[1].data()
        strSqlValores = f"SELECT * " \
                        f" FROM '{strTableName}'"
        self.modelValores.setQuery(strSqlValores, self.db)
        self.tableViewValores.setModel(self.modelValores)
        while self.tableViewValores.model().canFetchMore():
            self.tableViewValores.model().fetchMore()
        self.tableViewValores.verticalHeader().setDefaultSectionSize(14)
        self.tableViewValores.horizontalHeader().setVisible(True)
        self.tableViewValores.horizontalHeader().setFixedHeight(14)
        self.tableViewValores.horizontalHeader().setDefaultAlignment(QtCore.Qt.AlignTop)
        self.tableViewValores.horizontalHeader().setStyleSheet(self.strTableViewStyleSheet)
        self.tableViewValores.resizeColumnsToContents()
        self.tableViewValores.selectRow(0)

    def refreshAll(self):
        self.refreshMain()
        self.refreshTables()
        self.refreshFields()
        self.refreshSyncState()

    def displayTableInfo(self):
        self.refreshFields()
        self.refreshValues()

    def editDB(self, clickedCell):
        intTableId = clickedCell.siblingAtColumn(0).data()
        intFieldId = clickedCell.siblingAtColumn(1).data()
        strTableName = clickedCell.siblingAtColumn(2).data()
        strTableKeyName = clickedCell.siblingAtColumn(3).data()
        strTableComment = clickedCell.siblingAtColumn(4).data()
        strFieldName = clickedCell.siblingAtColumn(5).data()
        strFieldKeyName = clickedCell.siblingAtColumn(6).data()
        strFieldComment = clickedCell.siblingAtColumn(7).data()
        blnTabla = True if clickedCell.column() < 4 else False
        selectedRow = clickedCell.row()
        dialogEditCampo = DatosCampos(intTableId, intFieldId, strTableName, strTableKeyName, strTableComment,
                                      strFieldName, strFieldKeyName, strFieldComment, docDB=self.db, isTable=blnTabla)
        dialogEditCampo.exec_()
        if dialogEditCampo.accepted:
            self.refreshAll()
        del dialogEditCampo
        self.tableViewDb.selectRow(selectedRow)

    def editTabla(self, clickedCell):
        intTableId = clickedCell.siblingAtColumn(0).data()
        selectedRow = clickedCell.row()
        strTableName = clickedCell.siblingAtColumn(1).data()
        strTableKeyName = clickedCell.siblingAtColumn(2).data()
        strTableComment = clickedCell.siblingAtColumn(3).data()
        intFieldId = ''
        strFieldName = ''
        strFieldKeyName = ''
        strFieldComment = ''
        dialogEditCampo = DatosCampos(intTableId, intFieldId, strTableName, strTableKeyName, strTableComment,
                                      strFieldName, strFieldKeyName, strFieldComment, self.db, True)
        dialogEditCampo.exec_()
        if dialogEditCampo.accepted:
            self.refreshAll()
            self.tableViewTablas.selectRow(selectedRow)
        del dialogEditCampo

    def editCampo(self, clickedCell):
        intTableId = clickedCell.siblingAtColumn(0).data()
        selectedRow = clickedCell.row()
        qryTable = QSqlQuery(f'SELECT _sys_Tables.Table_Name FROM _sys_Tables WHERE _sys_Tables.ID_Table = {intTableId}',
                             self.db)
        strTableName = ''
        if qryTable.record().count() > 0:
            if qryTable.next():
                strTableName = qryTable.value(0)
        strTableKeyName = ''
        strTableComment = ''
        intFieldId = clickedCell.siblingAtColumn(1).data()
        strFieldName = clickedCell.siblingAtColumn(2).data()
        strFieldKeyName = clickedCell.siblingAtColumn(3).data()
        strFieldComment = clickedCell.siblingAtColumn(4).data()
        dialogEditCampo = DatosCampos(intTableId, intFieldId, strTableName, strTableKeyName, strTableComment,
                                      strFieldName, strFieldKeyName, strFieldComment, self.db)
        dialogEditCampo.exec_()
        if dialogEditCampo.accepted:
            self.refreshAll()
            self.tableViewCampos.selectRow(selectedRow)
        del dialogEditCampo

    def addTableKeyName(self, clickedCell):
        selectedRow = clickedCell.row()
        strTableName = clickedCell.data()
        strSQL = f"SELECT ID_Table, Table_Name, Table_Key_Name, Comments" \
                 f" FROM _sys_Tables" \
                 f" WHERE Table_Name = '{strTableName}'"
        qry = QSqlQuery(strSQL, self.db)
        if qry.next():
            intTableId = qry.value(0)
            strTableKeyName = qry.value(2)
            strTableComment = qry.value(3)
        intFieldId = ''
        strFieldName = ''
        strFieldKeyName = ''
        strFieldComment = ''
        dialogEditCampo = DatosCampos(intTableId, intFieldId, strTableName, strTableKeyName, strTableComment,
                                      strFieldName, strFieldKeyName, strFieldComment, self.db, True)
        dialogEditCampo.lblCampo.setHidden(True)
        dialogEditCampo.lblKeyNameCampo.setText('Key &Name de la Tabla:')
        dialogEditCampo.lblComentariosCampo.setText('Co&mentarios de la Tabla:')
        dialogEditCampo.setWindowTitle('Datos de la tabla')
        dialogEditCampo.exec_()
        if dialogEditCampo.accepted:
            self.refreshAll()
            self.tableViewTablas.selectRow(selectedRow)
        del dialogEditCampo

    def addFieldKeyName(self, clickedCell):
        selectedRow = clickedCell.row()
        strTableName = clickedCell.data().split('.')[0]
        strFieldName = clickedCell.data().split('.')[1]
        strSQL = f"SELECT ID_Table FROM _sys_Tables WHERE Table_Name = '{strTableName}'"
        qry = QSqlQuery(strSQL, self.db)
        if qry.next():
            intTableId = qry.value(0)
        strSQL = f"SELECT ID_Field, Field_Name, Field_Key_Name, Comments" \
                 f" FROM _sys_Fields" \
                 f" WHERE ID_Table = {intTableId} AND Field_Name = '{strFieldName}' "
        qry = QSqlQuery(strSQL, self.db)
        if qry.next():
            intFieldId = qry.value(0)
            strFieldKeyName = qry.value(2)
            strFieldComment = qry.value(3)
        strTableKeyName = ''
        strTableComment = ''
        dialogEditCampo = DatosCampos(intTableId, intFieldId, strTableName, strTableKeyName, strTableComment,
                                      strFieldName, strFieldKeyName, strFieldComment, self.db)
        dialogEditCampo.exec_()
        if dialogEditCampo.accepted:
            self.refreshAll()
            self.tableViewCampos.selectRow(selectedRow)
        del dialogEditCampo

    def refreshSyncState(self):
        strSQLTablasWork = f"SELECT tName FROM main.sqlite_master WHERE type='table' " \
                      f" AND tName NOT LIKE 'sqlite_%' AND tName NOT LIKE '_sys_%' " \
                      f" and tName NOT IN (SELECT Table_Name FROM _sys_Tables) "
        strSQLTablasDoc = f"SELECT Table_Name FROM _sys_Tables " \
                      f" WHERE Table_Name NOT IN " \
                      f"   (SELECT tName FROM main.sqlite_master WHERE type='table' AND tName NOT LIKE 'sqlite_%')"
        strSQLTablasSinKeyName = f"SELECT tName FROM main.sqlite_master WHERE type='table' AND tName NOT LIKE 'sqlite_%' " \
                      f" and tName IN (SELECT Table_Name FROM _sys_Tables " \
                      f"              WHERE _sys_Tables.Table_Key_Name  = '' " \
                      f"              OR _sys_Tables.Table_Key_Name  IS NULL " \
                      f"             )"
        strSQLCamposWork = f"SELECT m.tName || '.' || p.tName as Fields " \
                      f" FROM sqlite_master m " \
                      f" LEFT OUTER JOIN pragma_table_info((m.tName)) p " \
                      f"    on m.tName <> p.tName " \
                      f" WHERE m.tName NOT LIKE 'sqlite_%' AND m.tName NOT LIKE '_sys_%' AND m.type = 'table'  " \
                      f" AND Fields NOT IN  " \
                      f"     (SELECT _sys_Tables.Table_Name || '.' || _sys_Fields.Field_Name AS Fields " \
                      f"     FROM _sys_Tables " \
                      f"     INNER JOIN _sys_Fields on _sys_Tables.ID_Table = _sys_Fields.ID_Table) " \
                      f" ORDER BY Fields"
        strSQLCamposDoc = f" SELECT _sys_Tables.Table_Name || '.' || _sys_Fields.Field_Name AS Fields " \
                      f" FROM _sys_Tables " \
                      f" INNER JOIN _sys_Fields on _sys_Tables.ID_Table = _sys_Fields.ID_Table " \
                      f" WHERE Fields NOT IN " \
                      f"     (SELECT m.tName || '.' || p.tName as Fields " \
                      f"     FROM sqlite_master m " \
                      f"     LEFT OUTER JOIN pragma_table_info((m.tName)) p " \
                      f"        on m.tName <> p.tName " \
                      f"     WHERE m.tName NOT LIKE 'sqlite_%' AND m.type = 'table') " \
                      f" ORDER BY Fields"
        strSQLCamposSinKeyName = f"SELECT _sys_Tables.Table_Name || '.' || _sys_Fields.Field_Name AS Fields " \
                      f"FROM _sys_Tables " \
                      f"LEFT OUTER JOIN _sys_Fields on _sys_Tables.ID_Table = _sys_Fields.ID_Table " \
                      f"WHERE _sys_Fields.Field_Key_Name = '' OR _sys_Fields.Field_Key_Name IS NULL; "
        modelSync1 = QSqlQueryModel()
        modelSync1.setQuery(strSQLTablasWork, self.db)
        self.listViewTablasWork.setModel(modelSync1)
        modelSync2 = QSqlQueryModel()
        modelSync2.setQuery(strSQLTablasDoc, self.db)
        self.listViewTablasDoc.setModel(modelSync2)
        modelSync3 = QSqlQueryModel()
        modelSync3.setQuery(strSQLTablasSinKeyName, self.db)
        self.listViewTablasSinKeyName.setModel(modelSync3)
        modelSync4 = QSqlQueryModel()
        modelSync4.setQuery(strSQLCamposWork, self.db)
        self.listViewCamposWork.setModel(modelSync4)
        modelSync5 = QSqlQueryModel()
        modelSync5.setQuery(strSQLCamposDoc, self.db)
        self.listViewCamposDoc.setModel(modelSync5)
        modelSync6 = QSqlQueryModel()
        modelSync6.setQuery(strSQLCamposSinKeyName, self.db)
        self.listViewCamposSinKeyName.setModel(modelSync6)
        if modelSync1.rowCount() == 0:
            self.btnSincTablas1.setVisible(False)
        else:
            self.btnSincTablas1.setVisible(True)
        if modelSync2.rowCount() == 0:
            self.btnSincTablas2.setVisible(False)
        else:
            self.btnSincTablas2.setVisible(True)
        if modelSync3.rowCount() == 0:
            self.lblTablasSinKeyName.setText('Tablas sin Key Name')
        else:
            self.lblTablasSinKeyName.setText('Tablas sin Key Name (doble click para editar)')
        if modelSync4.rowCount() == 0:
            self.btnSincCampos1.setVisible(False)
        else:
            self.btnSincCampos1.setVisible(True)
        if modelSync5.rowCount() == 0:
            self.btnSincCampos2.setVisible(False)
        else:
            self.btnSincCampos2.setVisible(True)
        if modelSync6.rowCount() == 0:
            self.lblCamposSinKeyName.setText('Campos sin Key Name')
        else:
            self.lblCamposSinKeyName.setText('Campos sin Key Name (doble click para editar)')
        if modelSync1.rowCount() == 0 and modelSync2.rowCount() == 0:
            self.btnRenamedTable.setVisible(False)
        else:
            self.btnRenamedTable.setVisible(True)
        if modelSync4.rowCount() == 0 and modelSync5.rowCount() == 0:
            self.btnRenamedField.setVisible(False)
        else:
            self.btnRenamedField.setVisible(True)
        if modelSync1.rowCount() == 0 and modelSync2.rowCount() == 0 \
                and modelSync4.rowCount() == 0 and modelSync5.rowCount() == 0:
            self.syncState = True
            self.lblStatus.setVisible(False)
        else:
            self.syncState = False
            self.lblStatus.setVisible(True)
        self.btnVerVisibility()
        self.renameButtonsVisibility()

    def btnVerVisibility(self):
        if not self.syncState:
            if self.tabSync.isHidden():
                self.lblStatus.setText('Hay datos para sincronizar.')
                self.btnVer.setVisible(True)
            else:
                self.lblStatus.setText('Hay datos para sincronizar - Doble click para sincronizar items individuales.')
                self.btnVer.setVisible(False)
        else:
            self.btnVer.setVisible(False)

    def btnVerClick(self):
        self.tabWidget.setCurrentWidget(self.tabSync)

    def btnSyncAllClick(self):
        self.btnSincTablas1Click()
        self.btnSincTablas2Click()
        self.btnSincCampos1Click()
        self.btnSincCampos2Click()

    def renameButtonsVisibility(self):
        if self.listViewTablasWork.model().rowCount() > 0 \
                and self.listViewTablasDoc.model().rowCount() > 0:
            if self.listViewTablasWork.model().index(0, 0).data() == self.listViewTablasDoc.model().index(0, 0).data():
                self.btnRenamedTable.setEnabled(False)
            else:
                self.btnRenamedTable.setEnabled(True)
        else:
            self.btnRenamedTable.setEnabled(False)
        if self.listViewCamposWork.model().rowCount() > 0 \
                and self.listViewCamposDoc.model().rowCount() > 0:
            strTbl1 = self.listViewCamposWork.model().index(0, 0).data().split('.')[0]
            strTbl2 = self.listViewCamposDoc.model().index(0, 0).data().split('.')[0]
            if strTbl1 == strTbl2:
                self.btnRenamedField.setEnabled(True)
            else:
                self.btnRenamedField.setEnabled(True)
        else:
            self.btnRenamedField.setEnabled(False)

    def btnRenamedTableClick(self):
        idxTbl1 = self.listViewTablasWork.currentIndex().row()
        idxTbl2 = self.listViewTablasDoc.currentIndex().row()
        strTbl1 = self.listViewTablasWork.model().index(idxTbl1, 0).data()
        strTbl2 = self.listViewTablasDoc.model().index(idxTbl1, 0).data()
        msg = QMessageBox()
        msg.setWindowIcon(self.windowIcon())
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle('Aviso')
        msg.setText(f"¿Actualizar el nombre de la tabla \"{strTbl2}\" de dev.db\na \"{strTbl1}\"?")
        msg.addButton('&Aceptar', QMessageBox.YesRole)
        msg.addButton('&Cancelar', QMessageBox.RejectRole)
        retval = msg.exec_()
        if retval == 0:  # Accepted
            qry = QSqlQuery(self.db)
            strSQL = f"UPDATE _sys_Tables SET Table_Name = '{strTbl1}' WHERE Table_Name = '{strTbl2}'"
            if not qry.exec_(strSQL):
                print(qry.lastQuery())
                print(qry.lastError().text())
                sys.exit(-1)
            self.refreshAll()
        else:
            print("No renombrar")

    def btnRenamedFieldClick(self):
        idxFld1 = self.listViewCamposWork.currentIndex().row()
        idxFld2 = self.listViewCamposDoc.currentIndex().row()
        strTbl1 = self.listViewCamposWork.model().index(idxFld1, 0).data().split('.')[0]
        strTbl2 = self.listViewCamposDoc.model().index(idxFld2, 0).data().split('.')[0]
        strFld1 = self.listViewCamposWork.model().index(idxFld1, 0).data().split('.')[1]
        strFld2 = self.listViewCamposDoc.model().index(idxFld2, 0).data().split('.')[1]
        msg = QMessageBox()
        msg.setWindowIcon(self.windowIcon())
        msg.setIcon(QMessageBox.Information)
        msg.setWindowTitle('Aviso')
        msg.setText(f"¿Actualizar el nombre del campo \"{strFld2}\" de dev.db\na \"{strFld1}?\"")
        msg.addButton('&Aceptar', QMessageBox.YesRole)
        msg.addButton('&Cancelar', QMessageBox.RejectRole)
        retval = msg.exec_()
        if retval == 0:  # Accepted
            qry = QSqlQuery(self.db)
            strSQL = f"SELECT ID_Table FROM _sys_Tables WHERE Table_Name = '{strTbl1}'"
            qry.exec_(strSQL)
            if qry.next():
                intIDTable = qry.value(0)
                StrSQL = f"UPDATE _sys_Fields SET Field_Name = '{strFld1}'" \
                         f" WHERE ID_Table = {intIDTable} AND Field_Name = '{strFld2}'"
                if not qry.exec_(StrSQL):
                    print(qry.lastQuery())
                    print(qry.lastError().text())
                    sys.exit(-1)
            self.refreshAll()
        else:
            print("No renombrar")

    def btnSincTablas1Click(self):
        # Tablas de GanadoSQLite.db que no están en dev.db - Agregar a dev.db
        lst = []
        for idx in range(self.listViewTablasWork.model().rowCount()):
            lst.append(self.listViewTablasWork.model().index(idx, 0).data())
        self.tablasWorkSync(lstTables=lst)

    def btnSincTablas2Click(self):
        # Tablas de dev.db que no están en GanadoSQLite.db - Eliminar de dev.db
        lst = []
        for idx in range(self.listViewTablasDoc.model().rowCount()):
            lst.append(self.listViewTablasDoc.model().index(idx, 0).data())
        self.tablasDocSync(lstTables=lst)

    def btnSincCampos1Click(self):
        # Campos de GanadoSQLite.db que no están en dev.db - Agregar a dev.db
        lst = []
        for idx in range(self.listViewCamposWork.model().rowCount()):
            lst.append(self.listViewCamposWork.model().index(idx, 0).data().split('.'))
        self.camposWorkSync(lstTableFields=lst)

    def btnSincCampos2Click(self):
        # Campos de dev.db que no están en GanadoSQLite.db - Eliminar de dev.db
        lst = []
        for idx in range(self.listViewCamposDoc.model().rowCount()):
            lst.append(self.listViewCamposDoc.model().index(idx, 0).data().split('.'))
        self.camposDocSync(lstTableFields=lst)

    def tablasWorkSync(self, clickedCell=None, lstTables=[]):
        # Tablas de GanadoSQLite.db que no están en dev.db - Agregar a dev.db
        # Se asume que si no existe la tabla, tampoco existen sus campos, por lo que también se los inserta.
        qry = QSqlQuery(self.db)
        if clickedCell:
            lstTables.append(clickedCell.data())
        dateTime = datetime.datetime.now()
        for strTableName in lstTables:
            # Tomar el último recordID de tabla e incrementarlo
            qry.exec_(f"SELECT MAX(ID_Table) FROM _sys_Tables")
            if qry.next():
                intNextTableID = qry.value(0) + 1
            # Tomar el último recordID de tabla e incrementarlo
            qry.exec_(f"SELECT MAX(ID_Field) FROM _sys_Fields")
            if qry.next():
                intNextFieldID = qry.value(0) + 1
            # Crear la lista de campos
            lstFields = []
            StrSQL = f"SELECT tName FROM pragma_table_info('{strTableName}')"
            if not qry.exec_(StrSQL):
                print(qry.lastQuery())
                print(qry.lastError().text())
                sys.exit(-1)
            else:
                while qry.next():
                    lstFields.append(qry.value(0))
            qry.exec_(f'BEGIN TRANSACTION;')
            # Insertar el nuevo registro para la tabla
            StrSQL = f"INSERT INTO _sys_Tables (ID_Table, Table_Name, TimeStamp, Introduced_In_Version)" \
                     f" VALUES ({intNextTableID}, '{strTableName}', '{dateTime}', '{self.dbVersion}')"
            if not qry.exec_(StrSQL):
                print(qry.lastQuery())
                print(qry.lastError().text())
                qry.exec_(f'COMMIT;')
                sys.exit(-1)
            # Insertar los registros para los campos de la tabla
            for strFieldName in lstFields:
                StrSQL = f"INSERT INTO _sys_Fields (ID_Field, ID_Table, Field_Name, TimeStamp," \
                         f" Introduced_In_Version)" \
                         f" VALUES ({intNextFieldID}, '{intNextTableID}', '{strFieldName}', '{dateTime}'," \
                         f" '{self.dbVersion}')"
                if not qry.exec_(StrSQL):
                    print(qry.lastQuery())
                    print(qry.lastError().text())
                    qry.exec_(f'COMMIT;')
                    sys.exit(-1)
                intNextFieldID += 1
            qry.exec_(f'COMMIT;')

        self.refreshSyncState()

    def tablasDocSync(self, clickedCell=None, lstTables=[]):
        # Tablas de dev.db que no están en GanadoSQLite.db - Eliminar de dev.db
        if clickedCell:
            lstTables.append(clickedCell.data())
        qry = QSqlQuery(self.db)
        qry.exec_(f'BEGIN TRANSACTION;')
        for strTableName in lstTables:
            StrSQL = f"DELETE FROM _sys_Tables WHERE Table_Name = '{strTableName}'"
            if not qry.exec_(StrSQL):
                print(qry.lastQuery())
                print(qry.lastError().text())
                sys.exit(-1)
        qry.exec_(f'COMMIT;')
        self.refreshSyncState()

    def camposWorkSync(self, clickedCell=None, lstTableFields=[]):
        # Campos de GanadoSQLite.db que no están en dev.db - Agregar a dev.db
        qry = QSqlQuery(self.db)
        if clickedCell:
            # strTableName, strFieldName = clickedCell.obj_data().split('.')
            lstTableFields.append(clickedCell.data().split('.'))
        for tblFld in lstTableFields:
            strSQL = f"SELECT ID_Table FROM _sys_Tables WHERE Table_Name = '{tblFld[0]}'"
            qry.exec_(strSQL)
            if qry.next():
                intIDTable = qry.value(0)
                dateTime = datetime.datetime.now()
                # Tomar el último recordID de tabla _sys_Fields e incrementarlo
                qry.exec_(f"SELECT MAX(ID_Field) FROM _sys_Fields")
                if qry.next():
                    intNextFieldID = qry.value(0) + 1
                StrSQL = f"INSERT INTO _sys_Fields (ID_Field, ID_Table, Field_Name, TimeStamp, Introduced_In_Version)" \
                         f" VALUES ({intNextFieldID}, {intIDTable}, '{tblFld[1]}', '{dateTime}', '{self.dbVersion}')"
                if not qry.exec_(StrSQL):
                    print(qry.lastQuery())
                    print(qry.lastError().text())
                    sys.exit(-1)
            else:
                strError = f"La tabla {tblFld[0]} no existe en dev.db.\n¿Agregar la tabla?"
                msg = QMessageBox()
                msg.setWindowIcon(self.windowIcon())
                msg.setIcon(QMessageBox.Information)
                msg.setWindowTitle('Aviso')
                msg.setText(strError)
                msg.addButton('&Aceptar', QMessageBox.YesRole)
                msg.addButton('&Cancelar', QMessageBox.RejectRole)
                retval = msg.exec_()
                if retval == 0:  # Accepted
                    self.tablasWorkSync(lstTables=[tblFld[0]])
                else:
                    return
                if not lstTableFields:
                    return
        lstTableFields.clear()
        self.refreshSyncState()

    def camposDocSync(self, clickedCell=None, lstTableFields=[]):
        # Campos de dev.db que no están en GanadoSQLite.db - Eliminar de dev.db
        qry = QSqlQuery(self.db)
        if clickedCell:
            # strTableName, strFieldName = clickedCell.obj_data().split('.')
            lstTableFields.append(clickedCell.data().split('.'))
        for tblFld in lstTableFields:
            strSQL = f"SELECT ID_Table FROM _sys_Tables WHERE Table_Name = '{tblFld[0]}'"
            qry.exec_(strSQL)
            if qry.next():
                intIDTable = qry.value(0)
                StrSQL = f"DELETE FROM _sys_Fields" \
                         f" WHERE ID_Table = '{intIDTable}' AND Field_Name = '{tblFld[1]}'"
                if not qry.exec_(StrSQL):
                    print(qry.lastQuery())
                    print(qry.lastError().text())
                    sys.exit(-1)
        self.refreshSyncState()

    def fieldsToCSV(self):
        """
        Export list of hdrFields to CSV file
        :return: None
        """
        res = QFileDialog.getSaveFileName(self, "Guardar archivo CSV", ".", "Archivo CSV (*.csv)")
        fName = res[0]
        if fName == '':
            return
        tableList = []
        strToWriteLst = []
        qry1 = QSqlQuery(self.db)
        qry2 = QSqlQuery(self.db)
        # make tables list
        strSQL = f"SELECT tName FROM sqlite_schema" \
                 f" WHERE type ='table' AND tName NOT LIKE 'sqlite_%' AND tName NOT LIKE '_sys_%'" \
                 f" ORDER BY tName;"
        if not qry1.exec_(strSQL):
            print(qry1.lastQuery())
            print(qry1.lastError().text())
            sys.exit(-1)
        else:
            while qry1.next():
                tableList.append(qry1.value(0))
                # print(qry1.val(0))
        # process each table of the list
        for tbl in tableList:
            # take table recordID from _sys_Tables
            strSQL = f"SELECT ID_Table FROM _sys_Tables WHERE Table_Name = '{tbl}'"
            if not qry1.exec_(strSQL):
                print(qry1.lastQuery())
                print(qry1.lastError().text())
                sys.exit(-1)
            else:
                qry1.next()
                tblID = qry1.value(0)
            # for each field, make string to be exported as a CSV line
            strSQL = f"select * from (pragma_table_info('{tbl}'))"
            if not qry1.exec_(strSQL):
                print(qry1.lastQuery())
                print(qry1.lastError().text())
                sys.exit(-1)
            else:
                while qry1.next():
                    fld = qry1.value(1)
                    strDataType = qry1.value('type')
                    strNotNull = "NOT NULL" if qry1.value('notnull') > 0 else ""
                    strDefault = qry1.value('dflt_value')
                    strPK = "PK" if qry1.value('pk') > 0 else ""
                    strSQL = f"SELECT ID_Field, Field_Key_Name, Comments FROM _sys_Fields WHERE ID_Table = '{tblID}'" \
                             f" and  Field_Name = '{fld}'"
                    if not qry2.exec_(strSQL):
                        print(qry2.lastQuery())
                        print(qry2.lastError().text())
                        sys.exit(-1)
                    else:
                        qry2.next()
                        fielID = qry2.value(0)
                        fieldKeyName = qry2.value(1)
                        comment = qry2.value(2)
                    strToWrite = f'"{tbl}","{fld}","{strPK}","{fieldKeyName}","{fielID}","{strDefault}",' \
                                 f'"{strNotNull}","{strDataType}","{comment}"'
                    strToWriteLst.append(strToWrite)
        # export to file
        if len(strToWriteLst) > 0:
            with open(fName, 'w') as file:
                file.write(f'"Tabla","Campo","PK","Key Name","Key Name UID","Default Value","NOT NULL","Data Type","Comment"')
                for item in strToWriteLst:
                    print(item)
                    file.write(f"\n{item}")
                file.write(f"\n")
            file.close()
            QMessageBox.information(self, "Datos exportados", f"Datos exportados a {fName}")

    def tablesToCSV(self):
        """
        Export list of tables to CSV file
        :return: None
        """
        res = QFileDialog.getSaveFileName(self, "Guardar archivo CSV", ".", "Archivo CSV (*.csv)")
        fName = res[0]
        if fName == '':
            return
        tableList = []
        strToWriteLst = []
        qry1 = QSqlQuery(self.db)
        qry2 = QSqlQuery(self.db)
        # make tables list
        strSQL = f"SELECT tName FROM sqlite_schema" \
                 f" WHERE type ='table' AND tName NOT LIKE 'sqlite_%' AND tName NOT LIKE '_sys_%'" \
                 f" ORDER BY tName;"
        if not qry1.exec_(strSQL):
            print(qry1.lastQuery())
            print(qry1.lastError().text())
            sys.exit(-1)
        else:
            while qry1.next():
                tableList.append(qry1.value(0))
                # print(qry1.val(0))
        # process each table of the list
        for tbl in tableList:
            # take table recordID from _sys_Tables
            strSQL = f"SELECT * FROM _sys_Tables WHERE Table_Name = '{tbl}'"
            if not qry1.exec_(strSQL):
                print(qry1.lastQuery())
                print(qry1.lastError().text())
                sys.exit(-1)
            else:
                qry1.next()
                tblID = qry1.value(0)
                tblKeyName = qry1.value(2)
                timeStamp = qry1.value(3)
                IIV = qry1.value(4)
                tableType = qry1.value(5)
                comment = qry1.value(6)
                # f'"ID_Table","Table_Name","Table_Key_Name","TimeStamp","Introduced_In_Version","Table_Type","Comment"'
                strToWrite = f'"{tblID}","{tbl}","{tblKeyName}","{timeStamp}","{IIV}","{tableType}","{comment}"'

                strToWriteLst.append(strToWrite)

        # export to file
        if len(strToWriteLst) > 0:
            with open(fName, 'w') as file:
                file.write(f'"ID_Table","Table_Name","Table_Key_Name","TimeStamp","Introduced_In_Version","Table_Type","Comment"')
                for item in strToWriteLst:
                    print(item)
                    file.write(f"\n{item}")
                file.write(f"\n")
            file.close()
            QMessageBox.information(self, "Datos exportados", f"Datos exportados a {fName}")


class DatosCampos(DialogBaseDatosCampos, DialogUIDatosCampos):
    def __init__(self, intTableId, intFieldId, strTableName, strTableKeyName, strTableComment,
                 strFieldName, strFieldKeyName, strFieldComment, docDB, isTable=False, parent=None):
        DialogBaseDbDocMain.__init__(self, parent)
        self.setupUi(self)
        self.intTableId = intTableId
        self.intFieldId = intFieldId
        self.docDB = docDB
        self.isTable = isTable
        self.accepted = False
        self.lblTabla.setText(f'Tabla: {strTableName}')
        self.lblCampo.setText(f'Campo: {strFieldName}')
        if self.isTable:
            self.lblCampo.setHidden(True)
            self.lblKeyNameCampo.setText('Key &Name de la Tabla:')
            self.lblComentariosCampo.setText('Co&mentarios de la Tabla:')
            self.setWindowTitle('Datos de la tabla')
            self.txtKeyName.setText(strTableKeyName)
            self.pteComentariosCampo.setPlainText(strTableComment)
        else:
            self.txtKeyName.setText(strFieldKeyName)
            self.pteComentariosCampo.setPlainText(strFieldComment)
        self.strKeyNameOrig = self.txtKeyName.text()
        self.strCommentOrig = self.pteComentariosCampo.toPlainText()
        self.btnOk.clicked.connect(self.commit)

    def commit(self):
        strKeyName = self.txtKeyName.text()
        # strKeyName = 'NULL' if obj.txtKeyName.text() == '' else obj.txtKeyName.text()
        strComment = self.pteComentariosCampo.toPlainText()
        # if strKeyName == '' and strComment == '':  # No obj_data
        #     obj.connClose()
        #     return
        if strKeyName == self.strKeyNameOrig and strComment == self.strCommentOrig:  # No obj_data change
            self.close()
            return
        qry = QSqlQuery(self.docDB)
        strPara = '' if self.isTable else ' para esta tabla'
        strSQL = ''
        strSameKey = ''
        if self.isTable:
            if strKeyName == self.strKeyNameOrig:
                strSameKey = f" AND Table_Key_Name <> '{self.strKeyNameOrig}'"
            qry.exec_(f"SELECT * FROM _sys_Tables WHERE Table_Key_Name NOT NULL "
                      f" AND Table_Key_Name <> '' AND Table_Key_Name = '{strKeyName}{strSameKey}'")
            if not qry.next():
                strSQL = f"UPDATE _sys_Tables " \
                         f" SET Table_Key_Name = '{strKeyName}', Comments = '{strComment}' " \
                         f" WHERE ID_Table = {self.intTableId}"
        else:
            if strKeyName == self.strKeyNameOrig:
                strSameKey = f" AND Field_Key_Name <> '{self.strKeyNameOrig}'"
            qry.exec_(f"SELECT * FROM _sys_Fields"
                      f" WHERE ID_Table = {self.intTableId} AND Field_Key_Name NOT NULL"
                      f" AND Field_Key_Name <> '' AND Field_Key_Name = '{strKeyName}{strSameKey}'")
            if not qry.next():
                strSQL = f"UPDATE _sys_Fields " \
                         f" SET Field_Key_Name = '{strKeyName}', Comments = '{strComment}' " \
                         f" WHERE ID_Field = {self.intFieldId}"
        if strSQL == '':
            strError = f"El Key Name {strKeyName} ya existe{strPara}.\nIntente con un nombre diferente."
            msg = QMessageBox()
            msg.setWindowIcon(self.windowIcon())
            msg.setIcon(QMessageBox.Information)
            msg.setWindowTitle('Aviso')
            msg.setText(strError)
            msg.addButton('Aceptar', QMessageBox.YesRole)
            retval = msg.exec_()
        else:
            if not qry.exec_(strSQL):
                print(qry.lastError().text())
            self.accepted = True
            self.close()


# Usage
if __name__ == '__main__':
    app = QApplication(sys.argv)
    dialog = DbDoc()
    dialog.showMinimized()
    dialog.setWindowState(QtCore.Qt.WindowActive)
    dialog.show()
    sys.exit(app.exec_())
