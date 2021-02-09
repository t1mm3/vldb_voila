#ifndef H_PASS
#define H_PASS

#include <vector>
#include <string>
#include "voila.hpp"

struct IPass {
	struct LolepopCtx {
		Pipeline& p;
		Lolepop& l;
	};

protected:
	bool push_driven = true;

	Lolepop* move_to_next_op(Pipeline& p);
	Lolepop* get_next_op(Pipeline& p);
	Lolepop* get_prev_op(Pipeline& p);

	void reset() {
		lolepop_idx = 0;
	}

	int lolepop_idx = 0;

	static std::string var_name(const LolepopCtx& ctx, const std::string& id);


	virtual ~IPass() {
	}
public:
	std::string unique_id(const std::string& prefix = "__tmp", const std::string& postfix = "");
};

struct Pass : IPass {
protected:
	bool flat = false;

	typedef uint64_t RecurseFlags;

	static constexpr RecurseFlags kRecurse_Pred = 1 << 1;
	static constexpr RecurseFlags kRecurse_ExprArgs = 1 << 2;
	static constexpr RecurseFlags kRecurse_Default = kRecurse_Pred | kRecurse_ExprArgs;

	virtual void on_program(Program& p) { recurse_program(p); }
	virtual void recurse_program(Program& p,
			RecurseFlags flags = kRecurse_Default);

	virtual void on_pipeline(Pipeline& p) { recurse_pipeline(p); }
	virtual void recurse_pipeline(Pipeline& p,
			RecurseFlags flags = kRecurse_Default);

	virtual void on_lolepop(Pipeline& p, Lolepop& l) { recurse_lolepop(p, l); }
	virtual void recurse_lolepop(Pipeline& p, Lolepop& l,
			RecurseFlags flags = kRecurse_Default);

	virtual void on_statement(LolepopCtx& ctx, StmtPtr& s) {
		recurse_statement(ctx, s, true);
		recurse_statement(ctx, s, false);
	}

	virtual void recurse_statement(LolepopCtx& ctx, StmtPtr& s, bool pre,
		RecurseFlags flags = kRecurse_Default);

	virtual void on_expression(LolepopCtx& ctx, ExprPtr& s) { recurse_expression(ctx, s); }
	virtual void recurse_expression(LolepopCtx& ctx, ExprPtr& s,
		RecurseFlags flags = kRecurse_Default);

	virtual void on_data_structure(DataStructure& d) { (void)d; }

	virtual void on_done() {}
public:
	virtual void operator()(Program& p);
};


#include <unordered_set>
#include <unordered_map>

// Find matching references
struct FindRefs : Pass {
	std::unordered_set<std::string> needles;
	std::unordered_map<std::string, ExprPtr> references;

	void on_expression(LolepopCtx& ctx, ExprPtr& s) override {
		if (found_all()) {
			return;
		}

		recurse_expression(ctx, s);

		if (found_all()) {
			return;
		}

		if (s->type == Expression::Type::Reference) {
			auto it = needles.find(s->fun);
			if (it != needles.end()) {
				references[s->fun] = s;
				needles.erase(it);
			} else {
				// printf("Nothing found in ref %s\n", s->fun.c_str());
			}
		}
	}

	FindRefs(const std::vector<std::string>& refs) {
		for (auto& r : refs) {
			needles.insert(r);
		}
	}

	bool found_all() const {
		return !needles.size();
	}
};

struct RefCountingPass : Pass {
	std::unordered_map<ExprPtr, std::vector<NodePtr>> expr_nodes;
	std::unordered_map<std::string, std::vector<NodePtr>> var_nodes;
	std::vector<ExprPtr> lole_args;

	void on_pipeline(Pipeline& p) override;
	void on_statement(IPass::LolepopCtx& ctx, StmtPtr& s) override;
	void on_expression(IPass::LolepopCtx& ctx, ExprPtr& s) override;


	size_t get_count(const ExprPtr& e) const;
	size_t get_count(const std::string& e) const;

	RefCountingPass() {
		flat = true;	
	}
private:
	void add_expr(ExprPtr& e, NodePtr source);
	void add_var(const std::string& s, NodePtr source);
};


#include <memory>
struct PassPipeline : Pass {
	std::vector<std::unique_ptr<Pass>> passes;

	virtual void operator()(Program& p) override;
};

#endif