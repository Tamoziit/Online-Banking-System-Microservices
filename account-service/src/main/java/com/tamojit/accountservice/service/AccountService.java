package com.tamojit.accountservice.service;

import com.tamojit.accountservice.dto.AccountResponse;
import com.tamojit.accountservice.dto.CreateAccountRequest;
import com.tamojit.accountservice.model.Account;
import com.tamojit.accountservice.model.AccountStatus;
import com.tamojit.accountservice.model.AccountType;
import com.tamojit.accountservice.repository.AccountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.security.SecureRandom;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountService {
    private static final SecureRandom secureRandom = new SecureRandom();

    private final AccountRepository accountRepository;

    public AccountResponse createAccount(CreateAccountRequest request) {
        log.info("Creating acc. for: {}", request.getEmail());

        if (accountRepository.existsByEmail(request.getEmail())) {
            throw new RuntimeException("account already exists for email: " + request.getEmail());
        }

        Account account = new Account();
        account.setAccountHolderName(request.getAccountHolderName());
        account.setEmail(request.getEmail());
        account.setPhone(request.getPhone());
        account.setAccountType(request.getAccountType());
        account.setStatus(AccountStatus.ACTIVE);
        account.setBalance(request.getInitialDeposit());
        account.setAccountNumber(generateAccountNumber());
        account.setDailyTransactionLimit(
            request.getAccountType() == AccountType.SAVINGS
                ? new BigDecimal("100000")
                : new BigDecimal("500000")
        );

        Account savedAccount = accountRepository.save(account);
        log.info("Saved account for: {}", savedAccount.getAccountNumber());

        return mapToResponse(savedAccount);
    }

    public AccountResponse getAccount(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
            .orElseThrow(() -> new RuntimeException("account not found for account: " + accountNumber));

        return mapToResponse(account);
    }

    public BigDecimal getBalance(String accountNumber) {
        Account account = accountRepository.findByAccountNumber(accountNumber)
            .orElseThrow(() -> new RuntimeException("account not found for account: " + accountNumber));

        return account.getBalance();
    }

    public void blockAccount(String accountNumber) {
        log.info("Blocking account for: {}", accountNumber);

        Account account = accountRepository.findByAccountNumber(accountNumber)
            .orElseThrow(() -> new RuntimeException("account not found for account: " + accountNumber));

        account.setStatus(AccountStatus.BLOCKED);
        accountRepository.save(account);

        log.info("Blocked account for: {}", accountNumber);
    }

    public void deductBalance(String accountNumber, BigDecimal amount) {
        log.info("Deducting balance from account: {}, amt = {}", accountNumber, amount);

        Account account = accountRepository.findByAccountNumber(accountNumber)
            .orElseThrow(() -> new RuntimeException("Account not found for account: " + accountNumber));

        if (account.getStatus() != AccountStatus.ACTIVE) {
            throw new RuntimeException("Account status not active for account: " + accountNumber);
        }
        if (account.getBalance().compareTo(amount) < 0) {
            throw new RuntimeException("Insufficient Balance for account: " + accountNumber);
        }

        account.setBalance(account.getBalance().subtract(amount));
        accountRepository.save(account);

        log.info("Balance updated! New Balance: {}", account.getBalance());
    }

    public void creditBalance(String accountNumber, BigDecimal amount) {
        log.info("Crediting balance to account: {}, amt = {}", accountNumber, amount);

        Account account = accountRepository.findByAccountNumber(accountNumber)
            .orElseThrow(() -> new RuntimeException("Account not found for account: " + accountNumber));

        account.setBalance(account.getBalance().add(amount));
        accountRepository.save(account);

        log.info("Balance credited! New Balance: {}", account.getBalance());
    }

    private String generateAccountNumber() {
        String accountNumber;

        do {
            long number = secureRandom.nextLong(1_000_000_000_000L); // generated a random no. b/w 0 - (10^12 - 1)

            accountNumber = String.format("%012d", number); // pad the random nos. which are not 12-digit with 0s at the start
            // eg, if number = 12, then acc no. = 000000000012
        } while (accountRepository.existsByAccountNumber(accountNumber)); // check until unique acc. no. achieve

        return accountNumber;
    }

    private AccountResponse mapToResponse(Account account) {
        AccountResponse response = new AccountResponse();
        response.setId(account.getId());
        response.setAccountNumber(account.getAccountNumber());
        response.setAccountHolderName(account.getAccountHolderName());
        response.setEmail(account.getEmail());
        response.setPhone(account.getPhone());
        response.setAccountType(account.getAccountType());
        response.setStatus(account.getStatus());
        response.setBalance(account.getBalance());
        response.setDailyTransactionLimit(account.getDailyTransactionLimit());
        response.setCreatedAt(account.getCreatedAt());

        return response;
    }
}
