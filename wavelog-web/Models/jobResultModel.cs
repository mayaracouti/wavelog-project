namespace wavelog_web.Models
{
    public class JobResult
    {
        public bool Sucesso { get; set; }
        public string MensagemDeStatus { get; set; }
        public string LogsDaExecucao { get; set; }
    }
}