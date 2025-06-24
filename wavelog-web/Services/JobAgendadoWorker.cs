using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace wavelog_web.Services
{
    public class JobAgendadoWorker : BackgroundService
    {
        private readonly ILogger<JobAgendadoWorker> _logger;
        private readonly IServiceProvider _serviceProvider;
        private DateTime _proximaExecucao;

        public JobAgendadoWorker(ILogger<JobAgendadoWorker> logger, IServiceProvider serviceProvider)
        {
            _logger = logger;
            _serviceProvider = serviceProvider;
            _proximaExecucao = new DateTime(DateTime.Now.Year, DateTime.Now.Month, 1).AddMonths(1);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("======================================================");
            _logger.LogInformation("-> Robô de Agendamento iniciado.");
            _logger.LogInformation($"-> Próxima execução automática agendada para: {_proximaExecucao:dd/MM/yyyy}");
            _logger.LogInformation("======================================================");

            while (!stoppingToken.IsCancellationRequested)
            {
                if (DateTime.Now >= _proximaExecucao)
                {
                    _logger.LogInformation(">>> HORA DE EXECUTAR O JOB AGENDADO! <<<");

                    using (var scope = _serviceProvider.CreateScope())
                    {
                        var servicoDoJob = scope.ServiceProvider.GetRequiredService<ServicoDoJob>();
                        var resultado = await servicoDoJob.ExecutarProcessoDoJobAsync();

                        _logger.LogInformation("--- Resultado da Execução Automática ---");
                        _logger.LogInformation(resultado.LogsDaExecucao);
                        _logger.LogInformation("--- Fim da Execução Automática ---");
                    }

                    _proximaExecucao = new DateTime(DateTime.Now.Year, DateTime.Now.Month, 1).AddMonths(1);
                    _logger.LogInformation($"Próxima execução automática reagendada para: {_proximaExecucao:dd/MM/yyyy}");
                }

                await Task.Delay(TimeSpan.FromHours(1), stoppingToken);
            }
        }
    }
}
