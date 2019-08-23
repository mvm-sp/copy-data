select
	{idClient} as idclient,
     to_char(ae.dt_data, 'yyyy/mm') :: varchar(7) as date_mes,  
     fo.cd_fornecedor as cdagreement,
     fo.ds_fornecedor:: varchar(64) as agreement,
     pl.cd_plano  as cd_healthplan,
     pl.ds_plano :: varchar(64) as ds_healthplan,        
     ex.ds_tabela_cd :: varchar(64) as codigo_examprocedure,     
     pr.cd_procedimento as cd_examprocedure,                            
     pr.ds_procedimento :: varchar(64) as descr_examprocedure ,
     pg.cd_grupo as cd_modality , 
     pg.ds_grupo :: varchar(64) as modality ,
     ae.nr_controle as nr_reccontrol ,  
     ae.dt_data as dt_date , 
     pa.cd_paciente as cd_patient ,
     pa.ds_paciente as ds_patient , 
     ms.ds_crm_nr as crm_requester , 
     ms.ds_medico as name_requester ,    
     fm.ds_funcionario as schedule_name ,
     fr.ds_funcionario as receptionist_name ,
     ft.ds_funcionario as technical_name ,   
     me.ds_crm as crm_performer ,
     me.ds_guerra as name_performer ,       
     sa.ds_sala as ds_room ,
     '1' :: varchar(2) as nr_qte , 
     ex.nr_vl_co as nr_vl_co ,                               
     ex.nr_vl_hm as nr_vl_hm ,   
     ex.nr_vl_mf as nr_vl_mf ,   
     ex.nr_vl_ct as nr_vl_ct ,                                  
     ex.nr_vl_md as nr_vl_md ,
     ex.nr_vl_particular as nr_vl_personal ,                   
     (ex.nr_vl_particular + ex.nr_vl_convenio) as nr_total ,               
      ae.dt_hora as dt_time ,
      case  
     when dt_hora_senha < dt_hora_entrada then to_char('24:00'::time - dt_hora_entrada + dt_hora_senha,'HH24:MI')   
     when dt_hora_senha is not null and dt_hora_entrada is not null  
     then  to_char(dt_hora_senha - dt_hora_entrada, 'HH24:MI') end ::varchar(8) as ds_time_waiting ,
     em.ds_empresa  as ds_company ,		 
	case 
     when dt_hora_ficha < dt_hora_senha then to_char('24:00'::time - dt_hora_senha + dt_hora_ficha,'HH24:MI')   
     when dt_hora_ficha is not null and dt_hora_senha is not null  
     then  to_char(dt_hora_ficha - dt_hora_senha , 'HH24:MI') end ::varchar(8) as ds_time_form ,
    case  
     when dt_hora_sala02 < coalesce(null, dt_hora_sala01) then to_char('24:00'::time -coalesce(null, dt_hora_sala01) + dt_hora_sala02,'HH24:MI')   
     when dt_hora_sala02 is not null and coalesce(null, dt_hora_sala01) is not null  
     then  to_char(dt_hora_sala02 - coalesce(null, dt_hora_sala01), 'HH24:MI') end ::   varchar(8) as ds_time_execution ,
    case
     when coalesce(null, dt_hora_sala01)  > dt_hora  and dt_hora_ficha < coalesce(dt_hora_entrada,dt_hora)
     then  to_char( coalesce(null, dt_hora_sala01) - dt_hora , 'HH24:MI') end ::varchar(8) as ds_time_delay ,
    ae.dt_agendamento as dt_scheduling ,
(select ho.nr_horario 
from horarios ho
where ho.cd_horario = ae.cd_horario 
and to_char(ae.dt_data,'D')::integer = ho.nr_semana 
and case when exists (select cd_feriado from
feriados where dt_data = ae.dt_data 
              and (exists (select 1 from horarios where cd_sala =
ho.cd_sala and cd_feriado = feriados.cd_feriado) or
feriados.sn_feriado is true))
              then ho.cd_feriado = (select cd_feriado from feriados
where dt_data = ae.dt_data)
              else ho.cd_feriado is null end
	      and coalesce(ho.dt_entrada, ae.dt_data) <= ae.dt_data 
	      and coalesce(ho.dt_saida, ae.dt_data) >= ae.dt_data) as nr_schedule ,
coalesce(uc.nr_horario,pl.nr_horario) as ds_schedule_healthplan ,
exists (select 1 from estoques_controle join materiais using(cd_material) join materiais_grupos using(cd_grupo) where cd_exame = ex.cd_exame and sn_contraste is true) as sn_contrast ,
     ex.nr_vl_tx   as nr_vl_tx
FROM atendimentos          ae                                        
     join medicos          me using(cd_medico)                       
     join exames           ex using(cd_atendimento)                                                
     join planos           pl using(cd_plano)                        
     join pacientes        pa using(cd_paciente)                     
     join procedimentos    pr using(cd_procedimento)                 
     join modalidades      mo using(cd_modalidade) 
    left join unidade_convenio uc on (uc.cd_plano = pl.cd_plano and uc.cd_modalidade = mo.cd_modalidade)                   
     left join procedimentos_grupos pg using(cd_grupo)     
     join salas            sa using(cd_sala)                         
     join empresas         em on sa.cd_empresa = em.cd_empresa       
     join fornecedores     fo on pl.cd_fornecedor = fo.cd_fornecedor
left join medicos              ms on ms.cd_medico = ex.cd_medico
left join funcionarios fm     on (fm.cd_funcionario = ae.cd_funcionario_marcacao)
left join funcionarios fr     on (fr.cd_funcionario = ae.cd_funcionario_recepcao)
left join funcionarios ft     on (ft.cd_funcionario = ae.cd_funcionario_tecnico)       
WHERE ae.nr_controle is not null                                                             
and ae.dt_data >= (to_char(now()::date,'YYYYMM01')::date - interval '2 month')::date
and ae.dt_data < to_char(now()::date,'YYYYMM01')::date