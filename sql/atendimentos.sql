SELECT
	ae.dt_data,
	ae.dt_hora,
	COALESCE(ae.dt_agendamento :: DATE,(SELECT dt_log::DATE FROM log_clinux WHERE cd_key = ae.cd_lancamento AND ds_table = 'atendimentos' AND ds_operation = 'INSERT' LIMIT 1)) AS dt_agendamento,
	COALESCE(ae.dt_agendamento::TIME, (SELECT dt_log::TIME FROM log_clinux WHERE cd_key = ae.cd_lancamento AND ds_table = 'atendimentos' AND ds_operation = 'INSERT' LIMIT 1)) AS hr_agendamento,
	ae.nr_controle,
	pa.ds_paciente,
	sa.ds_sala,
	em.ds_empresa,
	aa.ds_aviso,
	au.ds_urgente,
	pr.ds_procedimento,
	fo.ds_fornecedor,
	pl.ds_plano,
	me.ds_medico,
	ms.ds_medico AS ds_solicitante,
	CASE
		WHEN ae.ds_status IS NULL THEN 'LIVRE'
		WHEN ae.nr_controle IS NOT NULL THEN 'ATENDIDO'
		ELSE
		CASE
			WHEN ae.ds_status = 1 THEN 'CANCELADO         '
			WHEN ae.ds_status = 2 THEN 'MARCADO           '
			WHEN ae.ds_status = 3 THEN 'CONFIRMADO        '
			WHEN ae.ds_status = 4 THEN 'BLOQUEADO         '
			WHEN ae.ds_status = 5 THEN 'ATENDIDO          '
			WHEN ae.ds_status = 6 THEN 'ENTREGA RESULTADO '
			WHEN ae.ds_status = 7 THEN 'MODIFICACAO DADOS '
			WHEN ae.ds_status = 8 THEN 'TRANSFER. HORARIO '
			WHEN ae.ds_status = 9 THEN 'IMPRESSAO FICHA   '
			WHEN ae.ds_status = 10 THEN 'DIGITACAO LAUDO   '
			WHEN ae.ds_status = 11 THEN 'RECEPCAO          '
			WHEN ae.ds_status = 12 THEN 'IMPRESSAO LAUDO   '
			WHEN ae.ds_status = 13 THEN 'IMPRESSAO ETIQUETA'
			WHEN ae.ds_status = 14 THEN 'CONGELADO         '
			WHEN ae.ds_status = 15 THEN 'ASSINATURA LAUDO  '
		END :: VARCHAR
	END :: VARCHAR(16) AS ds_horario,
	ae.nr_controle IS NOT NULL AS sn_atendido,
	ae.dt_confirmado IS NOT NULL
	AND (ae.dt_cancelado IS NULL
	OR ae.dt_confirmado > ae.dt_cancelado) AS sn_confirmado,
	ae.dt_cancelado IS NOT NULL
	AND
	CASE
		WHEN EXISTS (
			SELECT 1
		FROM
			atendimentos
		JOIN exames
				USING(cd_atendimento)
		WHERE
			cd_paciente = ae.cd_paciente
			AND (dt_data >= ae.dt_cancelado::DATE
			AND dt_data <= ae.dt_cancelado::DATE + 30 )
			AND cd_procedimento = ex.cd_procedimento
			AND atendimentos.cd_atendimento <> ae.cd_atendimento) THEN FALSE
		ELSE (ae.dt_confirmado IS NULL
		OR ae.dt_confirmado < ae.dt_cancelado)
	END AS sn_cancelado,
	ae.cd_atendimento IS NOT NULL
	AND ae.cd_horario IS NULL
	AND dt_data = dt_agendamento::DATE
	AND NOT EXISTS (
		SELECT 1
	FROM
		log_clinux
	WHERE
		cd_key = ae.cd_atendimento
		AND ds_table = 'atendimentos'
		AND ds_field = 'cd_horario' ) AS sn_encaixe,
	CASE
		WHEN ae.nr_controle IS NOT NULL THEN 'SHOW'
		WHEN ae.ds_status <> 1
		AND ae.nr_controle IS NULL
		AND EXISTS (
			SELECT 1
		FROM
			atendimentos
		WHERE
			ae.nr_controle IS NOT NULL
			AND dt_data = ae.dt_data
			AND cd_paciente = ae.cd_paciente) THEN 'DESISTENCIA'
		WHEN ae.ds_status <> 1
		AND ae.nr_controle IS NULL THEN 'NO-SHOW'
	END::VARCHAR ds_noshow,
	fc.sn_ag_nautorizacao IS TRUE
	AND it.sn_autorizacao IS NOT TRUE AS sn_autorizacao_agendamento,
	fc.sn_ex_nautorizacao IS TRUE
	AND it.sn_autorizacao IS NOT TRUE AS sn_autorizacao_atendimento,
	CASE
		WHEN ex.ds_autorizacao IS NOT NULL
		AND fc.sn_ag_nautorizacao IS TRUE
		AND it.sn_autorizacao IS NOT TRUE THEN (
			SELECT dt_log
		FROM
			log_clinux lc
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_operation = 'INSERT' ))
		WHEN ex.ds_autorizacao IS NOT NULL
		AND EXISTS (
			SELECT dt_log
		FROM
			log_clinux lc
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_field = 'ds_autorizacao' ) ) THEN (
			SELECT dt_log
		FROM
			log_clinux lc
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_field = 'ds_autorizacao' )
		LIMIT 1)
		WHEN ds_autorizacao IS NOT NULL THEN (
			SELECT dt_log
		FROM
			log_clinux lc
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_operation = 'INSERT' ))
	END::DATE AS dt_autorizacao,
	CASE
		WHEN ex.ds_autorizacao IS NOT NULL
		AND fc.sn_ag_nautorizacao IS TRUE
		AND it.sn_autorizacao IS NOT TRUE THEN (
			SELECT dt_log
		FROM
			log_clinux lc
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_operation = 'INSERT' ))
		WHEN ex.ds_autorizacao IS NOT NULL
		AND EXISTS (
			SELECT dt_log
		FROM
			log_clinux lc
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_field = 'ds_autorizacao' ) ) THEN (
			SELECT dt_log
		FROM
			log_clinux lc
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_field = 'ds_autorizacao' )
		LIMIT 1)
		WHEN ds_autorizacao IS NOT NULL THEN (
			SELECT dt_log
		FROM
			log_clinux lc
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_operation = 'INSERT' ))
	END::TIME AS hr_autorizacao,
	CASE
		WHEN ex.ds_autorizacao IS NOT NULL
		AND fc.sn_ag_nautorizacao IS TRUE
		AND it.sn_autorizacao IS NOT TRUE THEN (
			SELECT ds_funcionario
		FROM
			log_clinux lc
		JOIN funcionarios
				USING(cd_funcionario)
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_operation = 'INSERT' ))
		WHEN ex.ds_autorizacao IS NOT NULL
		AND EXISTS (
			SELECT ds_funcionario
		FROM
			log_clinux lc
		JOIN funcionarios
				USING(cd_funcionario)
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_field = 'ds_autorizacao' )) THEN (
			SELECT ds_funcionario
		FROM
			log_clinux lc
		JOIN funcionarios
				USING(cd_funcionario)
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_field = 'ds_autorizacao' )
		LIMIT 1)
		WHEN ds_autorizacao IS NOT NULL THEN (
			SELECT ds_funcionario
		FROM
			log_clinux lc
		JOIN funcionarios
				USING(cd_funcionario)
		WHERE
			lc.cd_key = ex.cd_exame
			AND lc.ds_table = 'exames'
			AND (ds_operation = 'INSERT' ))
	END ds_funcionario_autorizacao,
	CASE
		WHEN (ae.dt_cancelado IS NULL
		OR ae.dt_confirmado > ae.dt_cancelado) THEN fco.ds_funcionario::VARCHAR(64)
	END AS ds_funcionario_confirmacao,
	CASE
		WHEN (ae.dt_cancelado IS NULL
		OR ae.dt_confirmado > ae.dt_cancelado) THEN ae.dt_confirmado::DATE
	END AS dt_confirmado,
	CASE
		WHEN (ae.dt_cancelado IS NULL
		OR ae.dt_confirmado > ae.dt_cancelado) THEN ae.dt_confirmado::TIME
	END AS hr_confirmado,
	EXISTS (
		SELECT 1
	FROM
		setup
	WHERE
		cd_convenio_retorno = ex.cd_plano) AS sn_complemento,
	EXISTS (
		SELECT 1
	FROM
		atendimentos
	JOIN exames
			USING(cd_atendimento)
	WHERE
		cd_paciente = ae.cd_paciente
		AND (dt_data >= ae.dt_cancelado::DATE
		AND dt_data <= ae.dt_cancelado::DATE + 30 )
		AND cd_procedimento = ex.cd_procedimento
		AND atendimentos.cd_atendimento <> ae.cd_atendimento ) AS sn_reagendado,
	ae.dt_futurofone,
	CASE
		WHEN ae.ds_futurofone :: INT = 1 THEN 'CONFIRMADO'
		WHEN ae.ds_futurofone :: INT = 2 THEN 'CANCELADO'
		WHEN ae.ds_futurofone :: INT = 3 THEN 'ATENDENTE'
		WHEN ae.ds_futurofone :: INT = 4 THEN 'REJEITADO'
		WHEN ae.ds_futurofone :: INT = 5 THEN 'NAO ATENDEU'
	END :: VARCHAR AS ds_statusfone,
	sn_sms_confirmacao,
	ae.dt_sms_agenda AS dt_sms,
	CASE
		WHEN ae.ds_status = 1
		AND ae.ds_futurofone::INT = 2 THEN 'PREDITIVO'
		WHEN ae.ds_status = 1 THEN ac.ds_cancelamento
	END::VARCHAR(30) AS ds_cancelamento,
	CASE
		WHEN ae.ds_status = 1 THEN ae.dt_cancelado
	END AS dt_cancelado,
	ap.ds_procedencia :: VARCHAR(64) AS ds_procedencia
FROM
	atendimentos AE
JOIN medicos ME
		USING (cd_medico)
JOIN salas SA
		USING (cd_sala)
JOIN empresas em ON
	sa.cd_empresa = em.cd_empresa
JOIN pacientes PA
		USING (cd_paciente)
JOIN exames EX
		USING (cd_atendimento)
JOIN procedimentos PR
		USING (cd_procedimento)
JOIN planos PL ON
	EX.cd_plano = PL.cd_plano
JOIN fornecedores fo ON
	(fo.cd_fornecedor = pl.cd_fornecedor)
LEFT JOIN tabelas ta ON
	(ta.cd_tabela = pl.cd_tabela)
LEFT JOIN tabela_indice it ON
	(it.cd_tabela = ta.cd_tabela
	AND COALESCE(pr.cd_pai, pr.cd_procedimento) = it.cd_procedimento)
LEFT JOIN fornecedores_campos fc ON
	(fc.cd_fornecedor = fo.cd_fornecedor
	AND fc.cd_modalidade = pr.cd_modalidade)
LEFT JOIN medicos ms ON
	EX.cd_medico = ms.cd_medico
LEFT JOIN atendimentos_avisos aa ON
	ae.nr_aviso = aa.cd_aviso
LEFT JOIN atendimentos_urgentes au ON
	(au.cd_urgente = ae.nr_urgente)
LEFT JOIN funcionarios fco ON
	(fco.cd_funcionario = ae.cd_funcionario_confirmacao)
LEFT JOIN atendimentos_cancelamentos ac ON
	(ac.cd_cancelamento = ae.cd_cancelamento)
LEFT JOIN atendimentos_procedencias ap ON
	(ap.cd_procedencia = AE.cd_procedencia)
WHERE  ae.dt_data >= (to_char(now()::date,'YYYYMM01')::date - interval '2 day')::date and ae.dt_data < to_char(now()::date,'YYYYMM01')::date