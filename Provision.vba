Sub ActualizarTarifasDesdeBase()

    ' Declaración de variables
    Dim wsTarifario As Worksheet, wsAumentos As Worksheet, wsBase As Worksheet
    Dim dictAumentos As Object, dictBase As Object
    Dim lastRowAumentos As Long, lastRowBase As Long, lastRowTarifario As Long
    Dim i As Long, j As Long
    Dim key As String, acumulado As Double
    Dim baseValores As Variant

    ' Asignación de hojas
    Set wsTarifario = ThisWorkbook.Sheets("Tarifario")
    Set wsAumentos = ThisWorkbook.Sheets("Aumentos")
    Set wsBase = ThisWorkbook.Sheets("B_Tarifas")

    ' Inicialización de diccionarios
    Set dictAumentos = CreateObject("Scripting.Dictionary")
    Set dictBase = CreateObject("Scripting.Dictionary")

    ' ================================
    ' 1. Calcular acumulado desde columnas H a S y guardar en columna G
    ' ================================
    lastRowAumentos = wsAumentos.Cells(wsAumentos.Rows.Count, 1).End(xlUp).Row
    For i = 12 To lastRowAumentos
        acumulado = 1
        For j = 8 To 19 ' Columnas H a S
            If IsNumeric(wsAumentos.Cells(i, j).Value) Then
                acumulado = acumulado * (1 + wsAumentos.Cells(i, j).Value)
            End If
        Next j
        wsAumentos.Cells(i, 7).Value = Round(acumulado - 1, 6) ' Columna G
        key = wsAumentos.Cells(i, 1).Value & "|" & wsAumentos.Cells(i, 5).Value & "|" & wsAumentos.Cells(i, 6).Value
        dictAumentos(key) = acumulado
    Next i

    ' ================================
    ' 2. Leer valores base desde hoja B_Tarifas
    ' ================================
    lastRowBase = wsBase.Cells(wsBase.Rows.Count, 1).End(xlUp).Row
    For i = 3 To lastRowBase
        key = wsBase.Cells(i, 1).Value & "|" & wsBase.Cells(i, 5).Value & "|" & wsBase.Cells(i, 12).Value
        dictBase(key) = Array( _
            wsBase.Cells(i, 6).Value, wsBase.Cells(i, 7).Value, wsBase.Cells(i, 8).Value, _
            wsBase.Cells(i, 9).Value, wsBase.Cells(i, 10).Value, _
            wsBase.Cells(i, 15).Value, wsBase.Cells(i, 20).Value _
        )
    Next i

    ' ================================
    ' 3. Aplicar aumentos en hoja Tarifario
    ' ================================
    lastRowTarifario = wsTarifario.Cells(wsTarifario.Rows.Count, 1).End(xlUp).Row
    For i = 11 To lastRowTarifario
        key = wsTarifario.Cells(i, 1).Value & "|" & wsTarifario.Cells(i, 5).Value & "|" & wsTarifario.Cells(i, 12).Value
        If dictAumentos.exists(key) And dictBase.exists(key) Then
            acumulado = dictAumentos(key)
            baseValores = dictBase(key)

            ' Si es Directo, actualizar columnas F:J
            If wsTarifario.Cells(i, 12).Value = "Directo" Then
                For j = 0 To 4
                    If IsNumeric(baseValores(j)) Then
                        wsTarifario.Cells(i, 6 + j).Value = Round(baseValores(j) * (1 + acumulado), 2)
                    End If
                Next j

            ' Si es Distribución, actualizar columnas O y T
            ElseIf wsTarifario.Cells(i, 12).Value = "Distribucion" Then
                If IsNumeric(baseValores(5)) Then
                    wsTarifario.Cells(i, 15).Value = Round(baseValores(5) * (1 + acumulado), 2)
                End If
                If IsNumeric(baseValores(6)) Then
                    wsTarifario.Cells(i, 20).Value = Round(baseValores(6) * (1 + acumulado), 2)
                End If
            End If
        End If
    Next i

    ' ================================
    ' 4. Finalización
    ' ================================
    MsgBox "Tarifas actualizadas correctamente desde B_Tarifas."

End Sub


'-------------------------------------------------------------------------------------------------------
'-------------------------------------------------------------------------------------------------------
'-------------------------------------------------------------------------------------------------------

