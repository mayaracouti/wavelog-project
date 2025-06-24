using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;
using wavelog_web.Services;

namespace wavelog_web.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class JobController : ControllerBase
    {
        private readonly ServicoDoJob _servicoDoJob;

        public JobController(ServicoDoJob servicoDoJob)
        {
            _servicoDoJob = servicoDoJob;
        }

        [HttpGet("status")]
        public IActionResult Status()
        {
            return Ok(new
            {
                status = "Pronto para executar o job.",
                horario = DateTime.Now
            });
        }

        [HttpPost("executar")]
        public async Task<IActionResult> ExecutarJob()
        {
            var resultado = await _servicoDoJob.ExecutarProcessoDoJobAsync();

            return Ok(new
            {
                sucesso = resultado.Sucesso,
                mensagem = resultado.MensagemDeStatus,
                logs = resultado.LogsDaExecucao
            });
        }
    }
}