using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using wavelog_web.Models;

namespace wavelog_web.Services
{
    public class ServicoDoJob
    {
        private readonly IConfiguration _configuracoes;

        public ServicoDoJob(IConfiguration configuracoes)
        {
            _configuracoes = configuracoes;
        }

        public async Task<JobResult> ExecutarProcessoDoJobAsync()
        {
            var registrosLog = new StringBuilder();
            var resultado = new JobResult();

            try
            {
                string caminhoPython = _configuracoes["JobSettings:PythonExecutablePath"] ?? "python";
                string scriptRelativo = _configuracoes["JobSettings:ScriptPath"] ?? "Scripts/worker.py";

                string caminhoScript = Path.Combine(AppContext.BaseDirectory, scriptRelativo);

                string stringDeConexao = _configuracoes.GetConnectionString("DefaultConnection");
                var dadosConexao = new MySqlConnectionStringBuilder(stringDeConexao);

                string argumentos = $"\"{caminhoScript}\" " +
                                    $"--host \"{dadosConexao.Server}\" " +
                                    $"--database \"{dadosConexao.Database}\" " +
                                    $"--user \"{dadosConexao.UserID}\" " +
                                    $"--password \"{dadosConexao.Password}\"";

                registrosLog.AppendLine("Iniciando processo para executar o script Python...");
                registrosLog.AppendLine("----------------------------------------------------");

                var infoDoProcesso = new ProcessStartInfo
                {
                    FileName = caminhoPython,
                    Arguments = argumentos,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                    CreateNoWindow = true,
                    WorkingDirectory = AppContext.BaseDirectory
                };

                using (var processo = new Process { StartInfo = infoDoProcesso })
                {
                    processo.OutputDataReceived += (sender, args) =>
                    {
                        if (args.Data != null) registrosLog.AppendLine(args.Data);
                    };
                    processo.ErrorDataReceived += (sender, args) =>
                    {
                        if (args.Data != null) registrosLog.AppendLine($"ERRO: {args.Data}");
                    };

                    processo.Start();
                    processo.BeginOutputReadLine();
                    processo.BeginErrorReadLine();

                    await processo.WaitForExitAsync();

                    if (processo.ExitCode == 0)
                    {
                        resultado.Sucesso = true;
                        resultado.MensagemDeStatus = "Job executado com sucesso!";
                    }
                    else
                    {
                        resultado.Sucesso = false;
                        resultado.MensagemDeStatus = "Ocorreu uma falha durante a execução do script Python.";
                    }
                }
            }
            catch (Exception erro)
            {
                registrosLog.AppendLine("====================================================");
                registrosLog.AppendLine($"ERRO CRÍTICO NO C#: {erro.Message}");
                resultado.Sucesso = false;
                resultado.MensagemDeStatus = "Ocorreu um erro crítico na aplicação.";
            }

            resultado.LogsDaExecucao = registrosLog.ToString();
            return resultado;
        }
    }
}
