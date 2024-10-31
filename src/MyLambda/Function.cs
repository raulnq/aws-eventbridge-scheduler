using Amazon.Scheduler;
using Amazon.Scheduler.Model;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using System.Text.Json;
using System.Globalization;

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]
namespace MyLambda;

public class Function
{
    private readonly AmazonSchedulerClient _schedulerClient;
    private readonly string _targetArn;
    private readonly string _roleArn;

    public Function()
    {
        _schedulerClient = new AmazonSchedulerClient();
        _targetArn = Environment.GetEnvironmentVariable("TARGET_ARN")!;
        _roleArn = Environment.GetEnvironmentVariable("ROLE_ARN")!;
    }

    public record Payload(string Key);

    public async Task<APIGatewayProxyResponse> Produce(APIGatewayProxyRequest input, ILambdaContext context)
    {
        var request = new CreateScheduleRequest
        {
            Name = $"myschedule-{input.RequestContext.RequestId}",
            ScheduleExpression = $"at({DateTime.UtcNow.AddMinutes(5).ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture)})",
            GroupName = "myapp",
            State = ScheduleState.ENABLED,
            Target = new Target { Arn = _targetArn, RoleArn = _roleArn, Input = JsonSerializer.Serialize(new Payload(Guid.NewGuid().ToString())) },
            ActionAfterCompletion = ActionAfterCompletion.DELETE,
            FlexibleTimeWindow = new FlexibleTimeWindow
            {
                Mode = FlexibleTimeWindowMode.OFF,
            }
        };

        await _schedulerClient.CreateScheduleAsync(request);

        return new APIGatewayProxyResponse
        {
            StatusCode = 200,
            Headers = new Dictionary<string, string> { { "Content-Type", "application/json" } }
        };
    }

    public async Task Consume(Payload input, ILambdaContext context)
    {
        context.Logger.LogLine("Key: " + input.Key);
        await Task.CompletedTask;
    }
}
