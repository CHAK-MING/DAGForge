#pragma once

#include "dagforge/scheduler/task.hpp"

#include <variant>

namespace dagforge {

struct AddTaskEvent {
  ExecutionInfo exec_info;
};

struct RemoveTaskEvent {
  DAGId dag_id;
  TaskId task_id;
};

struct ShutdownEvent {};

using SchedulerEvent =
    std::variant<AddTaskEvent, RemoveTaskEvent, ShutdownEvent>;

} // namespace dagforge
