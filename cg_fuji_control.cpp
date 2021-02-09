#include "cg_fuji_control.hpp"
#include <sstream>
#include "cg_fuji.hpp"
#include "runtime.hpp"
#include "runtime_utils.hpp"

// #define trace1_printf(...) printf(__VA_ARGS__);
#define trace1_printf(...) 

#define EOL std::endl


void
FlowGenerator::clear()
{
	fragment.clear();
	m_last_state = nullptr;
}


FlowGenerator::~FlowGenerator()
{
	clear();
}

void
FlowGenerator::generate(GenCtx& ctx)
{
	clite::CGen gen(num_parallel, ctx.unique_prefix);
	clite::CGen::GenCtx gctx {ctx.decl, ctx.impl};

	gen.generate(fragment, gctx);
}

clite::Block*
FlowGenerator::last_state()
{
	if (!m_last_state) {
		m_last_state = fragment.new_block("last_state").get();
	}
	return m_last_state;
}

#if 0
void
FsmFlowGen::generate(GenCtx& ctx)
{
	const auto& unique_prefix = ctx.unique_prefix;
	std::string loop_epilogue(unique_prefix + "__epilogue");

	auto& impl = ctx.impl;

	bool create_fsm = num_parallel > 1;
	const std::string local_state_type(get_state_name(ctx, true));

	optimize();

	if (create_fsm) {

		// ASSERT(num_parallel > 1);

		// local state
		impl << local_state_type << " states[" << num_parallel << "] = {" << EOL;
		for (size_t i=0; i<num_parallel; i++) {
			if (i==0) {
				impl << "";
			} else {
				impl << ",";
			}
			impl << local_state_type << "{ *this, thread }";
		}
		impl << "};" << EOL;

	} else {
		impl << local_state_type << " state { *this, thread };" << EOL;
	}

	// thread-wide state
	impl << get_state_name(ctx, false) << " thread_state { *this, thread };" << EOL;

	// initialize
	generate_init(impl);


	// annotate states with ids
	std::unordered_map<State*, size_t> ids;
	ids.reserve(states.size());

	size_t first_real_state = 3;

	{
		size_t state_count = first_real_state;
		for (auto& s : states) {
			ids.insert({s.get(), state_count});
			state_count++;
		}
	}


	if (create_fsm) {
		// scheduler
		const std::string state_var("local_state.fsm_state");
		impl
			<< "size_t schedule_counter = 0;" << EOL
			<< "size_t schedule_num_running = " << num_parallel << ";" << EOL
			<< "size_t num_count = 0;" << EOL
			<< "int64_t schedule_lock_count = 0;" << EOL
			<< "int schedule_next_thread = -1;" << EOL
			<< "size_t schedule_idx = 0;" << EOL
			<< EOL 
			<< "while(1) {" << EOL
			<< "/* Schedule next LWT */" << EOL
			<< "if (LIKELY(schedule_lock_count == 0)) {" << EOL
			<< "  schedule_idx=schedule_counter;" << EOL
			<< "  if (LIKELY(schedule_next_thread < 0)) {" << EOL
			<< "    schedule_counter = (schedule_counter+1) % " << num_parallel << ";" << EOL
			<< "  } else {" << EOL
			<< "    schedule_idx = schedule_next_thread;" << EOL
			<< "    schedule_next_thread = -1;" << EOL
			<< "    DBG_ASSERT(schedule_idx >= 0 && schedule_idx < " << num_parallel << ");"
			<< "  }" << EOL
			<< "}" << EOL
			<< "DBG_ASSERT(schedule_idx >= 0 && schedule_idx < " << num_parallel << ");"
			<< local_state_type << "& local_state = states[schedule_idx];" << EOL
			<< EOL
			<< "LOG_SCHED_TRACE(\"LWT %d@%d: RUN %d local_state %p\\n\", schedule_idx, __LINE__, " << state_var << ", &states[schedule_idx]);" << EOL;

		impl << "switch (" << state_var << ") {" << EOL;

		impl
			<< "  case 0: /*INIT*/" << EOL
			<< state_var << " = " << first_real_state << ";" << EOL
			<< "LOG_SCHED_TRACE(\"LWT %d@%d: INIT\\n\", schedule_idx, __LINE__);" << EOL
			<< "break; " << EOL
			<< "  case 1: /*END*/" << EOL
			<< state_var << " = 2; ASSERT(schedule_num_running > 0); schedule_num_running--; " << EOL
			<< "LOG_SCHED_TRACE(\"LWT %d@%d: END. %d running\\n\", schedule_idx, __LINE__, schedule_num_running);" << EOL
			<< "break; " << EOL
			<< "  case 2: /*ZOMBIE*/" << EOL
			<< "LOG_SCHED_TRACE(\"LWT %d@%d: ZOMBIE. %d running\\n\", schedule_idx, __LINE__, schedule_num_running);" << EOL
			<<" if (!schedule_num_running) { goto " << loop_epilogue << ";} break; " << EOL;

		// generate states
		for (auto& state : states) {
			impl
				<< "  case " << ids[state.get()] << ": {" << EOL
				<< "/* '" << state->dbg_name << "' */" << EOL
				<< "LOG_SCHED_TRACE(\"LWT %d@%d: " << state->dbg_name << "\\n\", schedule_idx, __LINE__);" << EOL;

			for (auto& node : state->nodes) {
				switch (node->type) {
				case BackendNode::Type::kPlain: {
						auto txt = (PlainState*)node.get();
						impl << txt->impl;
						break;
					}

				case BackendNode::Type::kThreadWideState: {
						auto txt = (ThreadWideState*)node.get();
						impl << txt->impl_thread;
						break;
					}


				case BackendNode::Type::kGotoState: {
						auto go = (GotoState*)node.get();
						if (go->is_exit) {
							impl << "LOG_SCHED_TRACE(\"LWT %d@%d: Exit\\n\", schedule_idx, __LINE__);" << EOL
								<< state_var << " = " << 1 << ";" << EOL
								<< "break;" << EOL;
						} else {
							printf("find state '%s'\n", go->state->dbg_name.c_str());
							auto state_id_it = ids.find(go->state);
							ASSERT(state_id_it != ids.end());

							if (go->threading == BranchThreading::NeverYield) {
								impl << "schedule_next_thread = schedule_idx;" << EOL;
							}

							impl
								<< "/* goto '" << go->state->dbg_name << "'' */" << EOL
								<< "LOG_SCHED_TRACE(\"LWT %d@%d: Goto %d '" << go->state->dbg_name
									<< "' schedule_next_thread=%d\\n\", schedule_idx, __LINE__, "
									<< state_id_it->second << ", schedule_next_thread);" << EOL
								<< state_var << " = " << state_id_it->second << ";" << EOL
								<< "break;" << EOL;
						}
						break;
					}

				default:
					ASSERT(false && "Unsupported op");
					break;
				}
			}

			ASSERT(!state->nodes.size() || state->is_terminated());
			impl << "ASSERT(false && \"Unreachable\");" << EOL;
			impl << "  }" << EOL;
		}
		impl
			<< "  default: LOG_ERROR(\"Invalid state %d at LWT %d\", " << state_var << ", schedule_idx);" << EOL
			<< "    ASSERT(false && \"Invalid state\"); break;" << EOL
			<< "}; /* switch */" << EOL
			<< "}; /* while */" << EOL;
	} else {
		const std::string kStatePrefix("state_");
		impl
			<< "auto& local_state = state;" << EOL
			<< "static constexpr int schedule_idx = 0;" << EOL
			<< "  " << kStatePrefix << 0 << ": /*INIT*/" << EOL
			<< "goto " << kStatePrefix << first_real_state << ";" << EOL
			<< "  " << kStatePrefix << 1 << ": /*END*/" << EOL
			<< "goto " << loop_epilogue << ";" << EOL;

		// generate states
		for (auto& state : states) {
			impl
				<< "  " << kStatePrefix << ids[state.get()] << ": {" << EOL
				<< "/* '" << state->dbg_name << "' */" << EOL
				<< "LOG_SCHED_TRACE(\"LWT %d@%d: " << state->dbg_name << "\\n\", schedule_idx, __LINE__);" << EOL;

			for (auto& node : state->nodes) {
				switch (node->type) {
				case BackendNode::Type::kPlain: {
						auto txt = (PlainState*)node.get();
						impl << txt->impl;
						break;
					}

				case BackendNode::Type::kThreadWideState: {
						auto txt = (ThreadWideState*)node.get();
						impl << txt->impl_thread;
						break;
					}

				case BackendNode::Type::kGotoState: {
						auto go = (GotoState*)node.get();
						if (go->is_exit) {
							impl << "LOG_SCHED_TRACE(\"LWT %d@%d: Exit\\n\", schedule_idx, __LINE__);" << EOL;
							impl << "goto "  + loop_epilogue + ";" << EOL;
						} else {
							printf("find state '%s'\n", go->state->dbg_name.c_str());
							auto state_id_it = ids.find(go->state);
							ASSERT(state_id_it != ids.end());

							impl
								<< "/* goto '" << go->state->dbg_name << "'' */" << EOL
								<< "LOG_SCHED_TRACE(\"LWT %d@%d: Goto %d '" << go->state->dbg_name
									<< "'\\n\", schedule_idx, __LINE__, "
									<< state_id_it->second << ");" << EOL
								<< "goto " << kStatePrefix << state_id_it->second << ";" << EOL;
						}
						break;
					}

				default:
					ASSERT(false && "Unsupported op");
					break;
				}
			}

			ASSERT(state->is_terminated());
			impl << "ASSERT(false && \"Unreachable\");" << EOL;
			impl << "  }" << EOL;
		}
	}

	impl << " " << loop_epilogue << ": " << EOL;
	if (create_fsm) {
		impl << "  printf(\"num_count=%lld\\n\", num_count);" << EOL;
	}
	impl << "  return; " << EOL;
}

#endif
