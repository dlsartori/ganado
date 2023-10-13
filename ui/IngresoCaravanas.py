# Form implementation generated from reading ui file 'IngresoCaravanas.ui'
#
# Created by: PyQt6 UI code generator 6.4.2
#
# WARNING: Any manual changes made to this file will be lost when pyuic6 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt6 import QtCore, QtGui, QtWidgets


class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(431, 236)
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap("Ganado.ico"), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        Dialog.setWindowIcon(icon)
        self.btnCancel = QtWidgets.QPushButton(parent=Dialog)
        self.btnCancel.setGeometry(QtCore.QRect(260, 205, 75, 23))
        self.btnCancel.setObjectName("btnCancel")
        self.btnOk = QtWidgets.QPushButton(parent=Dialog)
        self.btnOk.setGeometry(QtCore.QRect(345, 205, 75, 23))
        self.btnOk.setDefault(True)
        self.btnOk.setObjectName("btnOk")
        self.grpCaracteristicas = QtWidgets.QGroupBox(parent=Dialog)
        self.grpCaracteristicas.setGeometry(QtCore.QRect(10, 5, 191, 121))
        self.grpCaracteristicas.setObjectName("grpCaracteristicas")
        self.lblTipo = QtWidgets.QLabel(parent=self.grpCaracteristicas)
        self.lblTipo.setGeometry(QtCore.QRect(10, 20, 55, 13))
        self.lblTipo.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblTipo.setObjectName("lblTipo")
        self.cboFormato = ComboTabla(parent=self.grpCaracteristicas)
        self.cboFormato.setGeometry(QtCore.QRect(67, 67, 115, 20))
        self.cboFormato.setObjectName("cboFormato")
        self.cboColor = ComboTabla(parent=self.grpCaracteristicas)
        self.cboColor.setGeometry(QtCore.QRect(67, 42, 115, 20))
        self.cboColor.setObjectName("cboColor")
        self.cboTipo = ComboTabla(parent=self.grpCaracteristicas)
        self.cboTipo.setGeometry(QtCore.QRect(67, 17, 115, 20))
        self.cboTipo.setObjectName("cboTipo")
        self.lblTecnologia = QtWidgets.QLabel(parent=self.grpCaracteristicas)
        self.lblTecnologia.setGeometry(QtCore.QRect(10, 95, 55, 13))
        self.lblTecnologia.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblTecnologia.setObjectName("lblTecnologia")
        self.lblColor = QtWidgets.QLabel(parent=self.grpCaracteristicas)
        self.lblColor.setGeometry(QtCore.QRect(10, 45, 55, 13))
        self.lblColor.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblColor.setObjectName("lblColor")
        self.cboTecnologia = ComboTabla(parent=self.grpCaracteristicas)
        self.cboTecnologia.setGeometry(QtCore.QRect(67, 92, 115, 20))
        self.cboTecnologia.setObjectName("cboTecnologia")
        self.lblFormato = QtWidgets.QLabel(parent=self.grpCaracteristicas)
        self.lblFormato.setGeometry(QtCore.QRect(10, 70, 55, 13))
        self.lblFormato.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblFormato.setObjectName("lblFormato")
        self.pteComentario = QtWidgets.QPlainTextEdit(parent=Dialog)
        self.pteComentario.setGeometry(QtCore.QRect(10, 145, 411, 56))
        self.pteComentario.setTabChangesFocus(True)
        self.pteComentario.setObjectName("pteComentario")
        self.lblComentario = QtWidgets.QLabel(parent=Dialog)
        self.lblComentario.setGeometry(QtCore.QRect(11, 127, 71, 16))
        self.lblComentario.setObjectName("lblComentario")
        self.grpIdentificador = QtWidgets.QGroupBox(parent=Dialog)
        self.grpIdentificador.setGeometry(QtCore.QRect(210, 5, 211, 121))
        self.grpIdentificador.setObjectName("grpIdentificador")
        self.lblNumeroInicio = QtWidgets.QLabel(parent=self.grpIdentificador)
        self.lblNumeroInicio.setGeometry(QtCore.QRect(15, 43, 31, 13))
        self.lblNumeroInicio.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblNumeroInicio.setObjectName("lblNumeroInicio")
        self.lblNumeroFin = QtWidgets.QLabel(parent=self.grpIdentificador)
        self.lblNumeroFin.setGeometry(QtCore.QRect(110, 43, 21, 13))
        self.lblNumeroFin.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblNumeroFin.setObjectName("lblNumeroFin")
        self.txtPrefijo = QtWidgets.QLineEdit(parent=self.grpIdentificador)
        self.txtPrefijo.setGeometry(QtCore.QRect(50, 90, 56, 20))
        self.txtPrefijo.setText("")
        self.txtPrefijo.setObjectName("txtPrefijo")
        self.lblPrefijo = QtWidgets.QLabel(parent=self.grpIdentificador)
        self.lblPrefijo.setGeometry(QtCore.QRect(12, 92, 36, 13))
        self.lblPrefijo.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblPrefijo.setObjectName("lblPrefijo")
        self.lblSufijo = QtWidgets.QLabel(parent=self.grpIdentificador)
        self.lblSufijo.setGeometry(QtCore.QRect(105, 92, 36, 13))
        self.lblSufijo.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblSufijo.setObjectName("lblSufijo")
        self.txtSufijo = QtWidgets.QLineEdit(parent=self.grpIdentificador)
        self.txtSufijo.setGeometry(QtCore.QRect(145, 90, 56, 20))
        self.txtSufijo.setText("")
        self.txtSufijo.setObjectName("txtSufijo")
        self.radIndividual = QtWidgets.QRadioButton(parent=self.grpIdentificador)
        self.radIndividual.setGeometry(QtCore.QRect(20, 20, 82, 17))
        self.radIndividual.setChecked(True)
        self.radIndividual.setObjectName("radIndividual")
        self.radLote = QtWidgets.QRadioButton(parent=self.grpIdentificador)
        self.radLote.setGeometry(QtCore.QRect(115, 20, 82, 17))
        self.radLote.setObjectName("radLote")
        self.spbFin = QtWidgets.QSpinBox(parent=self.grpIdentificador)
        self.spbFin.setGeometry(QtCore.QRect(133, 40, 66, 20))
        self.spbFin.setMinimum(2)
        self.spbFin.setMaximum(100000)
        self.spbFin.setProperty("value", 2)
        self.spbFin.setObjectName("spbFin")
        self.spbInicio = QtWidgets.QSpinBox(parent=self.grpIdentificador)
        self.spbInicio.setGeometry(QtCore.QRect(50, 40, 56, 20))
        self.spbInicio.setMinimum(1)
        self.spbInicio.setMaximum(99000)
        self.spbInicio.setObjectName("spbInicio")
        self.spbDigitos = QtWidgets.QSpinBox(parent=self.grpIdentificador)
        self.spbDigitos.setEnabled(False)
        self.spbDigitos.setGeometry(QtCore.QRect(120, 65, 41, 20))
        self.spbDigitos.setMinimum(1)
        self.spbDigitos.setMaximum(32)
        self.spbDigitos.setProperty("value", 1)
        self.spbDigitos.setObjectName("spbDigitos")
        self.chkCerosAdic = QtWidgets.QCheckBox(parent=self.grpIdentificador)
        self.chkCerosAdic.setGeometry(QtCore.QRect(15, 66, 101, 17))
        self.chkCerosAdic.setObjectName("chkCerosAdic")
        self.lblNombre = QtWidgets.QLabel(parent=self.grpIdentificador)
        self.lblNombre.setGeometry(QtCore.QRect(10, 43, 66, 13))
        self.lblNombre.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblNombre.setObjectName("lblNombre")
        self.txtId = QtWidgets.QLineEdit(parent=self.grpIdentificador)
        self.txtId.setGeometry(QtCore.QRect(80, 40, 120, 20))
        self.txtId.setText("")
        self.txtId.setObjectName("txtId")
        self.lblDigitos = QtWidgets.QLabel(parent=self.grpIdentificador)
        self.lblDigitos.setEnabled(False)
        self.lblDigitos.setGeometry(QtCore.QRect(160, 69, 36, 13))
        self.lblDigitos.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblDigitos.setObjectName("lblDigitos")
        self.spbInicio.raise_()
        self.lblNumeroInicio.raise_()
        self.lblNumeroFin.raise_()
        self.txtPrefijo.raise_()
        self.lblPrefijo.raise_()
        self.lblSufijo.raise_()
        self.txtSufijo.raise_()
        self.radIndividual.raise_()
        self.radLote.raise_()
        self.spbFin.raise_()
        self.spbDigitos.raise_()
        self.chkCerosAdic.raise_()
        self.lblNombre.raise_()
        self.txtId.raise_()
        self.lblDigitos.raise_()
        self.progressBar = QtWidgets.QProgressBar(parent=Dialog)
        self.progressBar.setGeometry(QtCore.QRect(10, 210, 246, 16))
        self.progressBar.setProperty("value", 24)
        self.progressBar.setObjectName("progressBar")
        self.lblStatus = QtWidgets.QLabel(parent=Dialog)
        self.lblStatus.setGeometry(QtCore.QRect(10, 209, 246, 16))
        self.lblStatus.setObjectName("lblStatus")
        self.lblTipo.setBuddy(self.cboTipo)
        self.lblTecnologia.setBuddy(self.cboTecnologia)
        self.lblColor.setBuddy(self.cboColor)
        self.lblFormato.setBuddy(self.cboFormato)
        self.lblComentario.setBuddy(self.pteComentario)
        self.lblNumeroInicio.setBuddy(self.spbInicio)
        self.lblNumeroFin.setBuddy(self.spbFin)
        self.lblPrefijo.setBuddy(self.txtPrefijo)
        self.lblSufijo.setBuddy(self.txtSufijo)
        self.lblNombre.setBuddy(self.txtId)
        self.lblDigitos.setBuddy(self.cboColor)

        self.retranslateUi(Dialog)
        self.chkCerosAdic.toggled['bool'].connect(self.spbDigitos.setEnabled) # type: ignore
        self.chkCerosAdic.toggled['bool'].connect(self.lblDigitos.setEnabled) # type: ignore
        QtCore.QMetaObject.connectSlotsByName(Dialog)
        Dialog.setTabOrder(self.cboTipo, self.cboColor)
        Dialog.setTabOrder(self.cboColor, self.cboFormato)
        Dialog.setTabOrder(self.cboFormato, self.cboTecnologia)
        Dialog.setTabOrder(self.cboTecnologia, self.radIndividual)
        Dialog.setTabOrder(self.radIndividual, self.radLote)
        Dialog.setTabOrder(self.radLote, self.txtId)
        Dialog.setTabOrder(self.txtId, self.spbInicio)
        Dialog.setTabOrder(self.spbInicio, self.spbFin)
        Dialog.setTabOrder(self.spbFin, self.chkCerosAdic)
        Dialog.setTabOrder(self.chkCerosAdic, self.spbDigitos)
        Dialog.setTabOrder(self.spbDigitos, self.txtPrefijo)
        Dialog.setTabOrder(self.txtPrefijo, self.txtSufijo)
        Dialog.setTabOrder(self.txtSufijo, self.pteComentario)
        Dialog.setTabOrder(self.pteComentario, self.btnCancel)
        Dialog.setTabOrder(self.btnCancel, self.btnOk)

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "Ingreso de Caravanas"))
        self.btnCancel.setText(_translate("Dialog", "Cancela&r"))
        self.btnOk.setText(_translate("Dialog", "&Aceptar"))
        self.grpCaracteristicas.setTitle(_translate("Dialog", "Características"))
        self.lblTipo.setText(_translate("Dialog", "&Tipo"))
        self.lblTecnologia.setText(_translate("Dialog", "T&ecnología"))
        self.lblColor.setText(_translate("Dialog", "&Color"))
        self.lblFormato.setText(_translate("Dialog", "&Formato"))
        self.lblComentario.setText(_translate("Dialog", "Co&mentario"))
        self.grpIdentificador.setTitle(_translate("Dialog", "Identificación"))
        self.lblNumeroInicio.setText(_translate("Dialog", "Inici&o"))
        self.lblNumeroFin.setText(_translate("Dialog", "Fi&n"))
        self.lblPrefijo.setText(_translate("Dialog", "&Prefijo"))
        self.lblSufijo.setText(_translate("Dialog", "&Sufijo"))
        self.radIndividual.setText(_translate("Dialog", "&Individual"))
        self.radLote.setText(_translate("Dialog", "&Lote"))
        self.chkCerosAdic.setText(_translate("Dialog", "&Llenar con ceros:"))
        self.lblNombre.setText(_translate("Dialog", "I&dentificador"))
        self.lblDigitos.setText(_translate("Dialog", "dígitos"))
        self.lblStatus.setText(_translate("Dialog", "Muestra:"))
from comboTabla import ComboTabla
