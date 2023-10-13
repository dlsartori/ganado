# Form implementation generated from reading ui file 'IngresoAnimales.ui'
#
# Created by: PyQt6 UI code generator 6.4.2
#
# WARNING: Any manual changes made to this file will be lost when pyuic6 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt6 import QtCore, QtGui, QtWidgets


class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.setEnabled(True)
        Dialog.resize(466, 232)
        Dialog.setBaseSize(QtCore.QSize(7, 0))
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap("Ganado.ico"), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        Dialog.setWindowIcon(icon)
        self.btnCancel = QtWidgets.QPushButton(parent=Dialog)
        self.btnCancel.setGeometry(QtCore.QRect(295, 200, 76, 23))
        self.btnCancel.setObjectName("btnCancel")
        self.btnOk = QtWidgets.QPushButton(parent=Dialog)
        self.btnOk.setGeometry(QtCore.QRect(380, 200, 76, 23))
        self.btnOk.setDefault(True)
        self.btnOk.setObjectName("btnOk")
        self.pteComentario = QtWidgets.QPlainTextEdit(parent=Dialog)
        self.pteComentario.setGeometry(QtCore.QRect(175, 130, 281, 64))
        self.pteComentario.setTabChangesFocus(True)
        self.pteComentario.setPlainText("")
        self.pteComentario.setObjectName("pteComentario")
        self.lblComentario = QtWidgets.QLabel(parent=Dialog)
        self.lblComentario.setGeometry(QtCore.QRect(175, 113, 71, 16))
        self.lblComentario.setObjectName("lblComentario")
        self.grpIdentificador = QtWidgets.QGroupBox(parent=Dialog)
        self.grpIdentificador.setGeometry(QtCore.QRect(535, 120, 61, 41))
        self.grpIdentificador.setObjectName("grpIdentificador")
        self.lblTipoDeEntrada = QtWidgets.QLabel(parent=Dialog)
        self.lblTipoDeEntrada.setGeometry(QtCore.QRect(17, 15, 90, 13))
        self.lblTipoDeEntrada.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblTipoDeEntrada.setObjectName("lblTipoDeEntrada")
        self.lblDuenio = QtWidgets.QLabel(parent=Dialog)
        self.lblDuenio.setGeometry(QtCore.QRect(242, 41, 90, 13))
        self.lblDuenio.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblDuenio.setObjectName("lblDuenio")
        self.chkPreniez = QtWidgets.QCheckBox(parent=Dialog)
        self.chkPreniez.setEnabled(True)
        self.chkPreniez.setGeometry(QtCore.QRect(175, 90, 70, 17))
        self.chkPreniez.setObjectName("chkPreniez")
        self.lblMarca = QtWidgets.QLabel(parent=Dialog)
        self.lblMarca.setGeometry(QtCore.QRect(242, 66, 90, 13))
        self.lblMarca.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblMarca.setObjectName("lblMarca")
        self.lblRaza = QtWidgets.QLabel(parent=Dialog)
        self.lblRaza.setGeometry(QtCore.QRect(17, 66, 90, 13))
        self.lblRaza.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblRaza.setObjectName("lblRaza")
        self.cboRaza = ComboTabla(parent=Dialog)
        self.cboRaza.setGeometry(QtCore.QRect(110, 63, 121, 20))
        self.cboRaza.setObjectName("cboRaza")
        self.lblCategoria = QtWidgets.QLabel(parent=Dialog)
        self.lblCategoria.setGeometry(QtCore.QRect(17, 41, 90, 13))
        self.lblCategoria.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblCategoria.setObjectName("lblCategoria")
        self.cboCategoria = ComboTabla(parent=Dialog)
        self.cboCategoria.setGeometry(QtCore.QRect(110, 38, 121, 20))
        self.cboCategoria.setObjectName("cboCategoria")
        self.lblEstablecimiento = QtWidgets.QLabel(parent=Dialog)
        self.lblEstablecimiento.setGeometry(QtCore.QRect(242, 15, 90, 13))
        self.lblEstablecimiento.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblEstablecimiento.setObjectName("lblEstablecimiento")
        self.cboEstablecimiento = ComboTabla(parent=Dialog)
        self.cboEstablecimiento.setGeometry(QtCore.QRect(335, 13, 121, 20))
        self.cboEstablecimiento.setObjectName("cboEstablecimiento")
        self.btnAdic = QtWidgets.QPushButton(parent=Dialog)
        self.btnAdic.setGeometry(QtCore.QRect(335, 87, 121, 23))
        self.btnAdic.setDefault(False)
        self.btnAdic.setObjectName("btnAdic")
        self.cboDuenio = ComboTabla(parent=Dialog)
        self.cboDuenio.setGeometry(QtCore.QRect(335, 38, 121, 20))
        self.cboDuenio.setObjectName("cboDuenio")
        self.cboMarca = ComboTabla(parent=Dialog)
        self.cboMarca.setGeometry(QtCore.QRect(335, 63, 121, 20))
        self.cboMarca.setObjectName("cboMarca")
        self.cboTipoDeEntrada = ComboTabla(parent=Dialog)
        self.cboTipoDeEntrada.setGeometry(QtCore.QRect(110, 13, 121, 20))
        self.cboTipoDeEntrada.setObjectName("cboTipoDeEntrada")
        self.grpIdentificacion = QtWidgets.QGroupBox(parent=Dialog)
        self.grpIdentificacion.setGeometry(QtCore.QRect(10, 115, 151, 80))
        self.grpIdentificacion.setObjectName("grpIdentificacion")
        self.btnCaravanas = QtWidgets.QPushButton(parent=self.grpIdentificacion)
        self.btnCaravanas.setGeometry(QtCore.QRect(15, 20, 121, 23))
        self.btnCaravanas.setObjectName("btnCaravanas")
        self.btnOtros = QtWidgets.QPushButton(parent=self.grpIdentificacion)
        self.btnOtros.setGeometry(QtCore.QRect(15, 50, 121, 23))
        self.btnOtros.setObjectName("btnOtros")
        self.lblComentario.setBuddy(self.pteComentario)
        self.lblTipoDeEntrada.setBuddy(self.cboTipoDeEntrada)
        self.lblDuenio.setBuddy(self.cboDuenio)
        self.lblMarca.setBuddy(self.cboMarca)
        self.lblRaza.setBuddy(self.cboRaza)
        self.lblCategoria.setBuddy(self.cboCategoria)
        self.lblEstablecimiento.setBuddy(self.cboEstablecimiento)

        self.retranslateUi(Dialog)
        QtCore.QMetaObject.connectSlotsByName(Dialog)
        Dialog.setTabOrder(self.cboTipoDeEntrada, self.cboCategoria)
        Dialog.setTabOrder(self.cboCategoria, self.chkPreniez)
        Dialog.setTabOrder(self.chkPreniez, self.cboRaza)
        Dialog.setTabOrder(self.cboRaza, self.cboEstablecimiento)
        Dialog.setTabOrder(self.cboEstablecimiento, self.cboDuenio)
        Dialog.setTabOrder(self.cboDuenio, self.cboMarca)
        Dialog.setTabOrder(self.cboMarca, self.btnCaravanas)
        Dialog.setTabOrder(self.btnCaravanas, self.btnAdic)
        Dialog.setTabOrder(self.btnAdic, self.pteComentario)
        Dialog.setTabOrder(self.pteComentario, self.btnCancel)
        Dialog.setTabOrder(self.btnCancel, self.btnOk)

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "Ingreso de Animales"))
        self.btnCancel.setText(_translate("Dialog", "Cancelar"))
        self.btnOk.setText(_translate("Dialog", "Aceptar"))
        self.lblComentario.setText(_translate("Dialog", "C&omentarios"))
        self.grpIdentificador.setTitle(_translate("Dialog", "Grupo"))
        self.lblTipoDeEntrada.setText(_translate("Dialog", "* &Tipo de Entrada"))
        self.lblDuenio.setText(_translate("Dialog", "&Dueño"))
        self.chkPreniez.setText(_translate("Dialog", "Pre&ñada"))
        self.lblMarca.setText(_translate("Dialog", "&Marca"))
        self.lblRaza.setText(_translate("Dialog", "* &Raza"))
        self.lblCategoria.setText(_translate("Dialog", "* Cate&goría"))
        self.lblEstablecimiento.setText(_translate("Dialog", "* &Establecimiento"))
        self.btnAdic.setText(_translate("Dialog", "Datos Adicional&es ..."))
        self.grpIdentificacion.setTitle(_translate("Dialog", "&Identificación"))
        self.btnCaravanas.setText(_translate("Dialog", "* &Caravanas ..."))
        self.btnOtros.setText(_translate("Dialog", "* &Otras señas ..."))
from comboTabla import ComboTabla
