using wavelog_web.Services;

var builder = WebApplication.CreateBuilder(args);


// CONFIGURAR SERVIÇOS
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddScoped<ServicoDoJob>();
builder.Services.AddHostedService<JobAgendadoWorker>();

var app = builder.Build();


// CONFIGURAR A APLICAÇÃO
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();
app.Run();