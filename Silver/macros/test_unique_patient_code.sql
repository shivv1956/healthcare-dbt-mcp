{% test unique_patient_code(model) %}

SELECT 
    patient_id, 
    code, 
    COUNT(*) AS count
FROM {{ model }}
GROUP BY patient_id, code
HAVING COUNT(*) > 1

{% endtest %}