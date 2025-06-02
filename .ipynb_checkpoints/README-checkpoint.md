Ejercicio 1


 1.
 Para los ids nulos ¿Qué sugieres hacer con ellos ?

    Considerando que esta columna puede ser la llave primaria de la tabla, no es viable rellenar con un valor por default,
   Si no fuera posible estandarizar y fijar la generación automatica desde la generación del insumo raw, evaluaría generarlo nuevamente
   en la etapa de limpieza (si este campo no es determinando para cruces dentro de las tablas de la empresa.)
   En caso contrario, evaluaría si es viable la discriminación de los ids nulos.
   
 3. Considerando las columnas name y company_id ¿Qué inconsistencias notas
 y como las mitigas?
    Identifico valores nulos y valores erroneos como  "*******", estos datos no parecen tener la estructura de protección por HASH,
    una propuesta sería tener valor por default como Nulos o "No reportado" con el fin de no perder el registro unicamente con un filtro.

 5. Para el resto de los campos ¿Encuentras valores atípicos y de ser así cómo
 procedes?
    En general los campos vienen con formato string, lo que no permite su correcta manupulación.

    Puntualizando en los campos de fecha, como el created_at y paid_at se encuentran 3 casuisticas
        fecha Nula, longitud incorrecta (20190516), Formato atipico (2019-02-27T00:00:00), esto dificultó su manipulación y genera
        la necesidad de realizar correcciones sobre el dato antes de realizar cálculos con ellos.

 7. ¿Qué mejoras propondrías a tu proceso ETL para siguientes versiones?
    -Conociendo los formatos de entrada de la información raw, integraría métodos para solventar de forma especifica.
    -Si el proceso fuera recurrente (mensual por ejemplo), adaptaría el proceso para leer y escribir sin perder historia
    -Con una estructura de datos gobernada, usaría decimal(xx,y) en lugar de float.
    -Generaría un método especifico para el formateo de datos al cual le aplicaría las reglas de negocio puntuales para cada caso, en lugar de
    tener redundancia repitiendo casos en más de un método.
    -Genería un punto de entrada para ejecutar el proceso completo desde ahí (Se deja sobre el notebook buscando que sea más visual).

    