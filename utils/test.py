class MiClase:
    def __init__(self, parametro1, parametro2, parametro3):
        self.parametro1 = parametro1
        self.parametro2 = parametro2
        self.parametro3 = parametro3

parametros = {
    "parametro1": 10,
    "parametro2": "hola",
    "parametro3": True
}

objeto = MiClase(**parametros)

print(vars(objeto))
