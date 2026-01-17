version_settings(constraint=">=0.22.2")
secret_settings(disable_scrub=True)
load("ext://uibutton", "cmd_button", "text_input")
load('ext://dotenv', 'dotenv')

# Load tilt env file if it exists
dotenv_path = ".tilt.env"
if os.path.exists(dotenv_path):
  dotenv(fn=dotenv_path)

# Configure trigger mode
true = ("true", "1", "yes", "t", "y")
trigger_mode = TRIGGER_MODE_MANUAL
if os.environ.get('TRIGGER_MODE_AUTO', '').lower() in true:
  trigger_mode = TRIGGER_MODE_AUTO

# Docker images
custom_build(
  ref="cva-enduro-worker:dev",
  command=["hack/build_docker.sh"],
  deps=["."],
)

# Load Kubernetes resources
k8s_yaml(kustomize("hack/kube/overlays/dev"))

# CVA Enduro resources
k8s_resource(
  "cva-enduro-worker",
  labels=["01-CVA"],
  trigger_mode=trigger_mode
)

# Other resources
k8s_resource("mysql", port_forwards="3306", labels=["02-Others"])
k8s_resource("temporal", labels=["02-Others"])
k8s_resource("temporal-ui", port_forwards="7440:8080", labels=["02-Others"])

# Tools
k8s_resource(
  "mysql-recreate-databases",
  labels=["03-Tools"],
  auto_init=False,
  trigger_mode=TRIGGER_MODE_MANUAL
)

# Buttons
cmd_button(
  "flush",
  argv=[
    "sh",
    "-c",
    "tilt trigger mysql-recreate-databases; \
    sleep 5; \
    tilt trigger temporal; \
    tilt trigger cva-enduro-worker;",
  ],
  location="nav",
  icon_name="delete",
  text="Flush"
)
