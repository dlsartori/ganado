# Form implementation generated from reading ui file 'Establecimientos.ui'
#
# Created by: PyQt6 UI code generator 6.6.1
#
# WARNING: Any manual changes made to this file will be lost when pyuic6 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt6 import QtCore, QtGui, QtWidgets


class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(297, 141)
        self.cboTipoDeEntrada = ComboTabla(parent=Dialog)
        self.cboTipoDeEntrada.setGeometry(QtCore.QRect(195, 45, 91, 20))
        self.cboTipoDeEntrada.setObjectName("cboTipoDeEntrada")
        self.lblTipoDeEntrada = QtWidgets.QLabel(parent=Dialog)
        self.lblTipoDeEntrada.setGeometry(QtCore.QRect(14, 47, 56, 13))
        self.lblTipoDeEntrada.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblTipoDeEntrada.setObjectName("lblTipoDeEntrada")
        self.btnCancel = QtWidgets.QPushButton(parent=Dialog)
        self.btnCancel.setGeometry(QtCore.QRect(125, 110, 76, 23))
        self.btnCancel.setObjectName("btnCancel")
        self.btnOk = QtWidgets.QPushButton(parent=Dialog)
        self.btnOk.setGeometry(QtCore.QRect(210, 110, 76, 23))
        self.btnOk.setDefault(True)
        self.btnOk.setObjectName("btnOk")
        self.lineEdit = QtWidgets.QLineEdit(parent=Dialog)
        self.lineEdit.setGeometry(QtCore.QRect(75, 15, 113, 20))
        self.lineEdit.setObjectName("lineEdit")
        self.lblTipoDeEntrada_2 = QtWidgets.QLabel(parent=Dialog)
        self.lblTipoDeEntrada_2.setGeometry(QtCore.QRect(14, 17, 56, 13))
        self.lblTipoDeEntrada_2.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblTipoDeEntrada_2.setObjectName("lblTipoDeEntrada_2")
        self.lineEdit_2 = QtWidgets.QLineEdit(parent=Dialog)
        self.lineEdit_2.setGeometry(QtCore.QRect(75, 45, 113, 20))
        self.lineEdit_2.setObjectName("lineEdit_2")
        self.cboTipoDeEntrada_2 = ComboTabla(parent=Dialog)
        self.cboTipoDeEntrada_2.setGeometry(QtCore.QRect(75, 75, 111, 20))
        self.cboTipoDeEntrada_2.setObjectName("cboTipoDeEntrada_2")
        self.lblTipoDeEntrada_3 = QtWidgets.QLabel(parent=Dialog)
        self.lblTipoDeEntrada_3.setGeometry(QtCore.QRect(15, 77, 56, 13))
        self.lblTipoDeEntrada_3.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblTipoDeEntrada_3.setObjectName("lblTipoDeEntrada_3")
        self.lblTipoDeEntrada.setBuddy(self.cboTipoDeEntrada)
        self.lblTipoDeEntrada_2.setBuddy(self.cboTipoDeEntrada)
        self.lblTipoDeEntrada_3.setBuddy(self.cboTipoDeEntrada)

        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "Dialog"))
        self.lblTipoDeEntrada.setText(_translate("Dialog", "&Superficie"))
        self.btnCancel.setText(_translate("Dialog", "Cancelar"))
        self.btnOk.setText(_translate("Dialog", "Aceptar"))
        self.lblTipoDeEntrada_2.setText(_translate("Dialog", "* &Nombre"))
        self.lblTipoDeEntrada_3.setText(_translate("Dialog", "&Provincia"))
from comboTabla import ComboTabla
