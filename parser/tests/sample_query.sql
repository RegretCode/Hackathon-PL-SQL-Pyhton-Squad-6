-- Complex SQL query example for the empresa database
WITH dept_stats AS (
    SELECT 
        d.id as dept_id,
        d.nome as dept_nome,
        COUNT(f.id) as num_funcionarios,
        AVG(f.salario) as media_salario
    FROM 
        departamentos d
    LEFT JOIN 
        funcionarios f ON d.id = f.departamento_id
    GROUP BY 
        d.id, d.nome
),
projeto_stats AS (
    SELECT 
        p.id as projeto_id,
        p.nome as projeto_nome,
        COUNT(a.funcionario_id) as num_alocacoes,
        AVG(a.horas_semanais) as media_horas,
        AVG(av.nota) as media_avaliacao
    FROM 
        projetos p
    LEFT JOIN 
        alocacoes a ON p.id = a.projeto_id
    LEFT JOIN 
        avaliacoes av ON p.id = av.projeto_id AND a.funcionario_id = av.funcionario_id
    GROUP BY 
        p.id, p.nome
)
SELECT 
    f.nome as funcionario,
    f.cargo,
    f.salario,
    ds.dept_nome as departamento,
    ds.num_funcionarios,
    ds.media_salario,
    p.nome as projeto,
    a.horas_semanais,
    ps.media_horas as media_horas_projeto,
    av.nota as avaliacao,
    ps.media_avaliacao as media_avaliacao_projeto,
    RANK() OVER (PARTITION BY f.departamento_id ORDER BY f.salario DESC) as rank_salario_depto,
    RANK() OVER (PARTITION BY a.projeto_id ORDER BY av.nota DESC) as rank_avaliacao_projeto
FROM 
    funcionarios f
JOIN 
    dept_stats ds ON f.departamento_id = ds.dept_id
LEFT JOIN 
    alocacoes a ON f.id = a.funcionario_id
LEFT JOIN 
    projetos p ON a.projeto_id = p.id
LEFT JOIN 
    avaliacoes av ON f.id = av.funcionario_id AND p.id = av.projeto_id
LEFT JOIN 
    projeto_stats ps ON p.id = ps.projeto_id
WHERE 
    f.ativo = TRUE
    AND f.salario > (SELECT AVG(salario) FROM funcionarios)
ORDER BY 
    ds.dept_nome, rank_salario_depto, rank_avaliacao_projeto;
