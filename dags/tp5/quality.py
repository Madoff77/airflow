"""
Contrôle qualité des données transformées et décision de branchement.

Le contrôle qualité ne fait pas échouer la tâche : il produit un rapport
structuré. C'est ce rapport qui pilote ensuite le branchement conditionnel :

- aucune anomalie  -> chargement des données ;
- au moins une anomalie -> on n'écrit rien dans la table métier et on trace.
"""

from tp5 import config
from tp5.logging_utils import get_logger

logger = get_logger("quality")


def _check_row(row):
    """
    Contrôle une ligne et renvoie la liste des raisons d'anomalie trouvées.

    Une liste vide signifie que la ligne est valide.
    """
    reasons = []

    # 1. Champs obligatoires présents et non nuls.
    for field in config.REQUIRED_ROW_FIELDS:
        if row.get(field) is None:
            reasons.append(f"champ obligatoire manquant ou nul: {field}")

    # 2. Valeurs numériques dans les bornes physiques plausibles.
    for field, bounds in config.QUALITY_RULES.items():
        value = row.get(field)
        if value is None:
            continue
        if value < bounds["min"] or value > bounds["max"]:
            reasons.append(
                f"{field}={value} hors bornes [{bounds['min']}, {bounds['max']}]"
            )

    return reasons


def run_quality_checks(ti):
    """
    Évalue chaque ligne transformée et publie un rapport qualité via XCom.

    Le rapport contient le statut global, les lignes valides et le détail
    des anomalies. Il est consommé par le branchement et la traçabilité.
    """
    prepared_weather_data = ti.xcom_pull(task_ids="transform_weather_data")

    if not prepared_weather_data:
        raise ValueError("Aucune donnée transformée à contrôler.")

    valid_rows = []
    anomalies = []

    for row in prepared_weather_data:
        reasons = _check_row(row)
        if reasons:
            anomaly = {"city": row.get("city"), "reasons": reasons}
            anomalies.append(anomaly)
            logger.error(
                "Anomalie qualité pour %s : %s",
                anomaly["city"],
                "; ".join(reasons),
            )
        else:
            valid_rows.append(row)
            logger.info("Ligne valide pour %s.", row.get("city"))

    status = "anomaly" if anomalies else "ok"
    report = {
        "status": status,
        "rows_received": len(prepared_weather_data),
        "valid_rows": valid_rows,
        "anomalies": anomalies,
    }

    logger.info(
        "Contrôle qualité terminé : statut=%s, reçues=%d, valides=%d, anomalies=%d.",
        status,
        report["rows_received"],
        len(valid_rows),
        len(anomalies),
    )
    return report


def decide_branch(ti):
    """
    Branchement conditionnel basé sur le rapport qualité.

    Renvoie l'identifiant de la prochaine tâche à exécuter :

    - données saines  -> chargement ;
    - anomalie détectée -> traçabilité de l'anomalie (pas de chargement).
    """
    report = ti.xcom_pull(task_ids="run_quality_checks")

    if not report:
        raise ValueError("Aucun rapport qualité disponible pour le branchement.")

    if report["status"] == "ok":
        logger.info("Qualité conforme : passage au chargement des données.")
        return config.TASK_LOAD_VALID_DATA

    logger.warning(
        "Anomalie qualité détectée : chargement bloqué, passage à la traçabilité."
    )
    return config.TASK_TRACE_QUALITY_ANOMALY
