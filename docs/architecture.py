"""Architecture diagram as code (https://diagrams.mingrammer.com).

Render:  docker run --rm -v "$PWD/docs:/out" -w /out python:3.12-slim \
           sh -c "apt-get update -qq && apt-get install -y -qq graphviz >/dev/null && pip install -q diagrams && python architecture.py"
"""
from diagrams import Cluster, Diagram, Edge
from diagrams.aws.compute import Lambda
from diagrams.aws.database import Dynamodb
from diagrams.aws.devtools import Codepipeline
from diagrams.aws.integration import Eventbridge, SimpleNotificationServiceSns, SimpleQueueServiceSqs
from diagrams.aws.management import Cloudwatch
from diagrams.aws.mobile import Amplify
from diagrams.aws.network import APIGateway
from diagrams.aws.security import Cognito, IAMRole
from diagrams.onprem.client import User
from diagrams.onprem.vcs import Github

graph_attr = {"fontsize": "22", "pad": "0.6", "nodesep": "0.7", "ranksep": "1.0", "splines": "spline"}

with Diagram("Module 8 - Serverless To-Do with Event-Driven Expiry (eu-north-1)",
             filename="architecture", outformat=["png"], show=False, direction="LR", graph_attr=graph_attr):
    user = User("User\n(browser)")
    inbox = User("User inbox")

    with Cluster("CI/CD"):
        gh_backend = Github("backend repo\n(SAM template)")
        gh_frontend = Github("frontend repo\n(React + Amplify Auth)")
        pipeline = Codepipeline("GitHub Actions\nOIDC role\nsam build / deploy")

    with Cluster("AWS account - eu-north-1 (fully serverless, no VPC)"):
        amplify = Amplify("Amplify Hosting\nmodule-8-serverless-todo-frontend")

        with Cluster("Authentication"):
            cognito = Cognito("Cognito User Pool\nmodule-8-user-pool")
            pre_signup = Lambda("PreSignUp\n(auto-confirm)")
            post_auth = Lambda("PostAuthentication\n(subscribe email)")

        with Cluster("API layer"):
            api = APIGateway("REST API (prod)\nCognito authorizer")
            with Cluster("CRUD Lambdas (least-privilege role each)"):
                crud = [Lambda("list-tasks"), Lambda("get-task"), Lambda("create-task"),
                        Lambda("update-task"), Lambda("delete-task")]

        table = Dynamodb("DynamoDB module-8-tasks\nPK USER#sub / SK TASK#id\nGSI1 status index\non-demand + Streams")

        with Cluster("Event-driven expiry pipeline"):
            router = Lambda("stream-router")
            fifo = SimpleQueueServiceSqs("SQS FIFO\nmodule-8-task-events.fifo\ngroup = TaskId")
            processor = Lambda("task-event-processor\nSCHEDULE / CANCEL / EXPIRE\n(idempotent)")
            scheduler = Eventbridge("EventBridge Scheduler\none-time at(deadline)\nper task")
            dlq = SimpleQueueServiceSqs("DLQs\n(FIFO + failed deliveries)")
            sched_role = IAMRole("scheduler\ndelivery role")

        sns = SimpleNotificationServiceSns("SNS\nmodule-8-task-notifications\n(filter policy per email)")
        cw = Cloudwatch("CloudWatch\nJSON logs, metrics,\nalarms, dashboard, X-Ray")

    gh_backend >> pipeline
    gh_frontend >> pipeline
    pipeline >> Edge(label="deploy stack", style="dashed") >> api
    pipeline >> Edge(label="manual deployment\n(branch env vars)", style="dashed") >> amplify

    user >> Edge(label="HTTPS") >> amplify
    user >> Edge(label="sign up / sign in") >> cognito
    cognito >> pre_signup
    cognito >> post_auth >> Edge(label="Subscribe") >> sns
    user >> Edge(label="ID token") >> api
    api >> Edge(label="validate JWT", style="dotted") >> cognito
    api >> crud
    for fn in crud:
        fn >> table

    table >> Edge(label="stream\nNEW_AND_OLD_IMAGES", color="darkgreen") >> router
    router >> Edge(label="SCHEDULE / CANCEL", color="darkgreen") >> fifo
    fifo >> Edge(color="darkgreen") >> processor
    processor >> Edge(label="create / update /\ndelete schedule", color="darkorange") >> scheduler
    scheduler >> Edge(label="EXPIRE at deadline", color="darkorange") >> fifo
    scheduler - Edge(style="dotted") - sched_role
    processor >> Edge(label="Pending -> Expired\n(conditional)", color="red") >> table
    processor >> Edge(label="publish", color="red") >> sns >> Edge(label="email") >> inbox
    fifo >> Edge(style="dotted") >> dlq
    processor >> Edge(style="dotted") >> cw
