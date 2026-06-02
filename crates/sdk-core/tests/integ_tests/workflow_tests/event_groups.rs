//! Verify that `EventGroupMarker`s attached to lang-side options propagate all the
//! way down to the server-side `Command`s issued by Core. One mocked test per command
//! kind we currently expose `groups` on: activity, child workflow, timer.
//!
//! Plus one end-to-end test against a real server, verifying that the markers also
//! land on the resulting `HistoryEvent` (i.e. the server persists what we send).

use crate::common::{CoreWfStarter, activity_functions::StdActivities, mock_sdk_cfg};
use temporalio_client::{UntypedWorkflow, WorkflowStartOptions};
use temporalio_common::{
    data_converters::RawValue,
    protos::{
        coresdk::AsJsonPayloadExt,
        temporal::api::{
            enums::v1::{CommandType, EventType},
            sdk::v1::{
                EventGroupMarker,
                event_group_marker::{Label, Variant},
            },
        },
    },
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, TimerOptions, WorkflowContext, WorkflowResult,
};
use temporalio_sdk_core::{
    replay::{DEFAULT_WORKFLOW_TYPE, canned_histories},
    test_help::MockPollCfg,
};

use std::time::Duration;

fn label_marker(id: &str, label: &str) -> EventGroupMarker {
    EventGroupMarker {
        variant: Some(Variant::Label(Label {
            id: id.to_string(),
            label: Some(label.as_json_payload().unwrap()),
        })),
    } as EventGroupMarker
}

#[tokio::test]
async fn pass_event_group_markers_on_schedule_activity() {
    let t = canned_histories::single_activity("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let wf_id = mock_cfg.hists[0].wf_id.clone();
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let expected_markers = vec![label_marker("activity-group", "activity-group-label")];

    let expected_for_assert = expected_markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::ScheduleActivityTask
                );
                assert_eq!(wft.commands[0].event_group_markers, expected_for_assert);
            })
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
                assert!(wft.commands[0].event_group_markers.is_empty());
            });
    });

    let mut worker = mock_sdk_cfg(mock_cfg, |_| {});

    #[workflow]
    struct ActivityWithGroupWorkflow {
        groups: Vec<EventGroupMarker>,
    }

    #[workflow_methods(factory_only)]
    impl ActivityWithGroupWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let groups = ctx.state(|wf| wf.groups.clone());
            ctx.start_activity(
                StdActivities::default,
                (),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .groups(groups)
                    .build(),
            )
            .await?;
            Ok(())
        }
    }

    worker
        .register_workflow_with_factory(move || ActivityWithGroupWorkflow {
            groups: expected_markers.clone(),
        })
        .unwrap();
    let task_queue = worker.inner_mut().task_queue().to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn pass_event_group_markers_on_start_child_workflow() {
    let wf_id = "1";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_child_workflow(wf_id);
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let expected_markers = vec![label_marker("child-group", "child-group-label")];

    let expected_for_assert = expected_markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::StartChildWorkflowExecution
                );
                assert_eq!(wft.commands[0].event_group_markers, expected_for_assert);
            })
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
                assert!(wft.commands[0].event_group_markers.is_empty());
            });
    });

    let mut worker = mock_sdk_cfg(mock_cfg, |_| {});

    #[workflow]
    struct ChildWithGroupWorkflow {
        child_wf_id: String,
        groups: Vec<EventGroupMarker>,
    }

    #[workflow_methods(factory_only)]
    impl ChildWithGroupWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let (child_wf_id, groups) = ctx.state(|wf| (wf.child_wf_id.clone(), wf.groups.clone()));
            ctx.start_child_workflow(
                UntypedWorkflow::new("child"),
                RawValue::new(vec![]),
                ChildWorkflowOptions {
                    workflow_id: child_wf_id,
                    groups,
                    ..Default::default()
                },
            )
            .await?;
            Ok(())
        }
    }

    let child_wf_id = wf_id.to_string();
    let groups_for_wf = expected_markers.clone();
    worker
        .register_workflow_with_factory(move || ChildWithGroupWorkflow {
            child_wf_id: child_wf_id.clone(),
            groups: groups_for_wf.clone(),
        })
        .unwrap();
    let task_queue = worker.inner_mut().task_queue().to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn pass_event_group_markers_on_start_timer() {
    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let wf_id = mock_cfg.hists[0].wf_id.clone();
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let expected_markers = vec![label_marker("timer-group", "timer-group-label")];

    let expected_for_assert = expected_markers.clone();
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(wft.commands[0].command_type(), CommandType::StartTimer);
                assert_eq!(wft.commands[0].event_group_markers, expected_for_assert);
            })
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
                assert!(wft.commands[0].event_group_markers.is_empty());
            });
    });

    let mut worker = mock_sdk_cfg(mock_cfg, |_| {});

    #[workflow]
    struct TimerWithGroupWorkflow {
        groups: Vec<EventGroupMarker>,
    }

    #[workflow_methods(factory_only)]
    impl TimerWithGroupWorkflow {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
            let groups = ctx.state(|wf| wf.groups.clone());
            ctx.timer(TimerOptions {
                duration: Duration::from_secs(1),
                groups,
                ..Default::default()
            })
            .await;
            Ok(())
        }
    }

    let groups_for_wf = expected_markers.clone();
    worker
        .register_workflow_with_factory(move || TimerWithGroupWorkflow {
            groups: groups_for_wf.clone(),
        })
        .unwrap();
    let task_queue = worker.inner_mut().task_queue().to_owned();
    worker
        .submit_wf(
            wf_type.to_owned(),
            vec![],
            WorkflowStartOptions::new(task_queue, wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

// Constants used by the real-server test below; defining them at module scope so the
// workflow body and the assertion can construct the same marker independently.
const PERSIST_TEST_MARKER_ID: &str = "persist-test";
const PERSIST_TEST_MARKER_LABEL: &str = "persist-test-label";

#[workflow]
#[derive(Default)]
pub(crate) struct ActivityEventGroupPersistsWf;

#[workflow_methods]
impl ActivityEventGroupPersistsWf {
    #[run(name = "event_group_markers_persist_to_history_events")]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.start_activity(
            StdActivities::default,
            (),
            ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                .groups(vec![label_marker(
                    PERSIST_TEST_MARKER_ID,
                    PERSIST_TEST_MARKER_LABEL,
                )])
                .build(),
        )
        .await?;
        Ok(())
    }
}

/// End-to-end: a marker attached to a `ScheduleActivity` command must also land on the
/// resulting `ActivityTaskScheduled` history event after the server persists it.
#[tokio::test]
async fn event_group_markers_persist_to_history_events() {
    let wf_name = "event_group_markers_persist_to_history_events";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker
        .register_workflow::<ActivityEventGroupPersistsWf>()
        .unwrap();

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();

    let history = starter.get_history().await;
    let scheduled_events: Vec<_> = history
        .events
        .into_iter()
        .filter(|e| e.event_type() == EventType::ActivityTaskScheduled)
        .collect();
    assert_eq!(scheduled_events.len(), 1);
    assert_eq!(
        scheduled_events[0].event_group_markers,
        vec![label_marker(
            PERSIST_TEST_MARKER_ID,
            PERSIST_TEST_MARKER_LABEL
        )]
    );
}
