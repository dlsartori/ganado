# Form implementation generated from reading ui file 'Test.ui'
#
# Created by: PyQt6 UI code generator 6.4.2
#
# WARNING: Any manual changes made to this file will be lost when pyuic6 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt6 import QtCore, QtGui, QtWidgets


class Ui_MainWindow(object):
    def setupUi(self, MainWindow):
        MainWindow.setObjectName("MainWindow")
        MainWindow.resize(927, 576)
        self.centralwidget = QtWidgets.QWidget(parent=MainWindow)
        self.centralwidget.setObjectName("centralwidget")
        self.gridLayout = QtWidgets.QGridLayout(self.centralwidget)
        self.gridLayout.setContentsMargins(2, 2, 2, 2)
        self.gridLayout.setSpacing(2)
        self.gridLayout.setObjectName("gridLayout")
        self.tabWidget = QtWidgets.QTabWidget(parent=self.centralwidget)
        self.tabWidget.setObjectName("tabWidget")
        self.tabAnimales = QtWidgets.QWidget()
        self.tabAnimales.setObjectName("tabAnimales")
        self.gridLayout_2 = QtWidgets.QGridLayout(self.tabAnimales)
        self.gridLayout_2.setContentsMargins(2, 2, 2, 2)
        self.gridLayout_2.setSpacing(2)
        self.gridLayout_2.setObjectName("gridLayout_2")
        self.horizontalLayout = QtWidgets.QHBoxLayout()
        self.horizontalLayout.setSpacing(2)
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.btnAgregar = QtWidgets.QPushButton(parent=self.tabAnimales)
        self.btnAgregar.setObjectName("btnAgregar")
        self.horizontalLayout.addWidget(self.btnAgregar)
        self.btnBorrar = QtWidgets.QPushButton(parent=self.tabAnimales)
        self.btnBorrar.setObjectName("btnBorrar")
        self.horizontalLayout.addWidget(self.btnBorrar)
        spacerItem = QtWidgets.QSpacerItem(40, 20, QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Minimum)
        self.horizontalLayout.addItem(spacerItem)
        self.gridLayout_2.addLayout(self.horizontalLayout, 0, 0, 1, 1)
        self.tableViewAnimales = QtWidgets.QTableView(parent=self.tabAnimales)
        self.tableViewAnimales.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.SingleSelection)
        self.tableViewAnimales.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectionBehavior.SelectRows)
        self.tableViewAnimales.setObjectName("tableViewAnimales")
        self.gridLayout_2.addWidget(self.tableViewAnimales, 1, 0, 1, 1)
        self.tabWidget.addTab(self.tabAnimales, "")
        self.tabGeo = QtWidgets.QWidget()
        self.tabGeo.setObjectName("tabGeo")
        self.tabWidget.addTab(self.tabGeo, "")
        self.gridLayout.addWidget(self.tabWidget, 0, 0, 1, 1)
        MainWindow.setCentralWidget(self.centralwidget)
        self.menubar = QtWidgets.QMenuBar(parent=MainWindow)
        self.menubar.setGeometry(QtCore.QRect(0, 0, 927, 21))
        self.menubar.setObjectName("menubar")
        MainWindow.setMenuBar(self.menubar)
        self.statusbar = QtWidgets.QStatusBar(parent=MainWindow)
        self.statusbar.setObjectName("statusbar")
        MainWindow.setStatusBar(self.statusbar)

        self.retranslateUi(MainWindow)
        self.tabWidget.setCurrentIndex(0)
        QtCore.QMetaObject.connectSlotsByName(MainWindow)

    def retranslateUi(self, MainWindow):
        _translate = QtCore.QCoreApplication.translate
        MainWindow.setWindowTitle(_translate("MainWindow", "Pruebas"))
        self.btnAgregar.setText(_translate("MainWindow", "&Agregar"))
        self.btnBorrar.setText(_translate("MainWindow", "&Borrar"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tabAnimales), _translate("MainWindow", "Animales"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tabGeo), _translate("MainWindow", "Geo"))
