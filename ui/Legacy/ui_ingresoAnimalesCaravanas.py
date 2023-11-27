#!/usr/bin/env python
# -*- coding: utf-8 -*-
from PyQt5.QtSql import QSqlDatabase, QSqlQueryModel
from PyQt5.QtWidgets import QApplication
from PyQt5 import QtCore, uic
import sys
# https://stackoverflow.com/questions/32914488/global-variables-between-different-modules
# import cfg  # bind objects referenced by variables in cfg to like-named variables in this module
# These objects from 'cfg' now have an additional reference here
# from cfg import getTableNameBrck as tbl
# from cfg import getFieldNameBrck as fld
# from cfg import createDbConn as createDbConn
from ui_ingresoCaravanas import DialogoInsertarCaravanas

# https://www.riverbankcomputing.com/static/Docs/PyQt5/designer.html#PyQt5.uic.loadUiType
# https://stackoverflow.com/questions/22663716/working-with-pyqt-and-qt-designer-ui-files
DialogUI, DialogBase = uic.loadUiType("IngresoAnimalesCaravanas.ui")

'''                -- viewCaravanasSinAsignar --
SELECT [ID_Caravana],
    [Numero Caravana] AS [Identificador],
    [Caravanas Tipos].[Tipo De Caravana] AS [Tipo],
    [Colores].[Nombre Color] AS [Color],
    [Caravanas Formato].[Formato De Caravana] AS [Formato],
    [Caravanas Registro De Actividades].[ID_Status Caravana]
FROM [Caravanas] 
LEFT JOIN [Colores]
    ON [Caravanas].[ID_Color] = [Colores].[ID_Color]  
LEFT JOIN [Caravanas Tipos]
    ON [Caravanas].[ID_Tipo De Caravana] = [Caravanas Tipos].[ID_Tipo De Caravana]   
LEFT JOIN [Caravanas Formato]
    ON [Caravanas].[ID_Formato De Caravana] = [Caravanas Formato].[ID_Formato De Caravana]
LEFT JOIN [Caravanas Registro De Actividades]
    ON [Caravanas].[ID_Caravana] = [Caravanas Registro De Actividades].[ID_Caravana]   
WHERE [Caravanas Registro De Actividades].[ID_Status Caravana] = 1
ORDER BY [Identificador];
'''

class DialogoAnimalesCaravanas(DialogBase, DialogUI):
    """
    Dialog for tag selection

    On accept, dialog is hidden and object's attributes are exposed to be queried, until object is destroyed (see usage section)
    """
    def __init__(self, tplCaravanas=(), parent=None):
        DialogBase.__init__(self, parent)
        self.setupUi(self)

        # Set attributes
        self.lstIDs = []
        if len(tplCaravanas) > 0:
            for item in tplCaravanas:
                self.lstIDs.append(item)
        self.accepted = False
        self.lastKey = -1

        # # Connect database
        # [self.db, self.dbGanadoConnCreated] = createDbConn("GanadoSQLite.db", "GanadoConnection")
        #
        # # Tag list widgets setup
        # self.modelLeft = QSqlQueryModel()
        # self.modelRight = QSqlQueryModel()
        # self.strSelect = f"SELECT * FROM [viewCaravanasSinAsignar]"

        self.tableViewLeft.setModel(self.modelLeft)
        self.tableViewRight.setModel(self.modelRight)
        self.tableViewLeft.verticalHeader().setDefaultSectionSize(14)
        self.tableViewLeft.horizontalHeader().setFixedHeight(14)
        self.tableViewLeft.horizontalHeader().setDefaultAlignment(QtCore.Qt.AlignTop)
        self.tableViewLeft.horizontalHeader().setStyleSheet("QHeaderView::section "
                                                            "{ background-color:rgb(240, 240, 240);"
                                                            " border-style: none; }")
        self.tableViewLeft.setColumnHidden(0, True)
        self.tableViewLeft.setColumnHidden(2, True)
        self.tableViewRight.verticalHeader().setDefaultSectionSize(14)
        self.tableViewRight.horizontalHeader().setFixedHeight(14)
        self.tableViewRight.horizontalHeader().setDefaultAlignment(QtCore.Qt.AlignTop)
        self.tableViewRight.horizontalHeader().setStyleSheet("QHeaderView::section "
                                                            "{ background-color:rgb(240, 240, 240);"
                                                            " border-style: none; }")

        # Fill list widgets
        # self.requery()

        # Signals - Slots
        # self.btnOk.clicked.connect(self.commit)
        # self.btnCancel.clicked.connect(self.cancel)
        # self.btnToRight.clicked.connect(self.toRight)
        # self.btnToLeft.clicked.connect(self.toLeft)
        # self.tableViewLeft.doubleClicked.connect(self.toRight)
        # self.tableViewRight.doubleClicked.connect(self.toLeft)
        # self.btnNewTag.clicked.connect(self.newTag)

    def setLeftModelQuery(self):
        """
        Updates left TableView model query

        :return: None
        """
        pass
        # if len(self.lstIDs) > 0:
        #     self.strWhere =  f" WHERE ID_Caravana NOT IN " \
        #                      f"{self.lstIDs.__repr__().replace('[', '(').replace(']', ')')} " if self.lstIDs else ""
        # else:
        #     self.strWhere = ""
        # strSQL = self.strSelect + self.strWhere
        # self.modelLeft.setQuery(strSQL, self.db)
        # self.tableViewLeft.resizeColumnsToContents()
        # self.tableViewLeft.setColumnHidden(0, True)
        # self.tableViewLeft.setColumnHidden(2, True)

    def setRightModelQuery(self):
        """
        Updates right TableView model query

        :return: None
        """
        pass
        # if len(self.lstIDs) > 0:
        #     self.strWhere = f" WHERE ID_Caravana IN " \
        #                     f"{self.lstIDs.__repr__().replace('[', '(').replace(']', ')')} " if self.lstIDs else ""
        # else:
        #     self.strWhere = " AND FALSE "
        # strSQL = self.strSelect +self.strWhere
        # self.modelRight.setQuery(strSQL, self.db)
        # self.tableViewRight.resizeColumnsToContents()
        # self.tableViewRight.setColumnHidden(0, True)
        # self.tableViewRight.setColumnHidden(2, True)

    def requery(self):
        """
        Updates both TableViews model query

        :return: None
        """
        pass
        # self.setLeftModelQuery()
        # self.setRightModelQuery()

    def toRight(self):
        """
        Moves selected item from left TableView to right TableView

        :return: None
        """
        pass
        # for row in self.tableViewLeft.selectionModel().selectedRows():
        #     self.lstIDs.append(str(row.data()))
        # self.tableViewLeft.selectionModel().reset()
        # self.requery()

    def toLeft(self):
        """
        Moves selected item from right TableView to left TableView

        :return: None
        """
        pass
        # for row in self.tableViewRight.selectionModel().selectedRows():
        #     self.lstIDs.remove(str(row.data()))
        # self.tableViewRight.selectionModel().reset()
        # self.requery()

    def newTag(self):
        """
        Invokes the dialog for insertion of new tags

        :return: None
        """
        pass
        # dlg = DialogoInsertarCaravanas()
        # if dlg.exec_():
        #     self.requery()

    def cancel(self):
        """
        Closes the dialog

        :return: None
        """
        self.close()

    def commit(self):
        """
        Hide the dialog

        :return: None
        """
        # 1) Validation → no validation needed

        # 2) Cleanup:
        #    - Close connections and destroy objects
        pass
        # self.accepted =True
        # if self.dbGanadoConnCreated:
        #     self.db.close()
        #     QSqlDatabase.removeDatabase("GanadoSQLite.db")
        # self.accept()


# Usage
if __name__ == '__main__':
    app = QApplication(sys.argv)
    # app.setStyle("windows")
    # dialog = DialogoAnimalesCaravanas()
    dialog = DialogoAnimalesCaravanas(tplCaravanas=('9', '11'))
    dialog.exec_()
    if dialog.accepted:
        print(dialog.lstIDs)
    del dialog

